<?php

/**
 * Advanced consumer operations.
 *
 * Demonstrates: pauseConsumer/resumeConsumer, push consumer CRUD
 * (createPushConsumer, pushConsumer), ConsumerConfig advanced fields
 * (sampleFrequency, backOff, maxRequestExpires, headersOnly),
 * fetchBytes, consumer info details (numPending, numAckPending, cluster info).
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\AckPolicy;
use Nats\Enum\DeliveryPolicy;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\FetchOptions;
use Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// Setup: create stream and publish messages
$stream = $js->createStream(new StreamConfig(
    name: 'TASKS',
    subjects: ['tasks.>'],
));

for ($i = 1; $i <= 30; $i++) {
    $js->publish('tasks.process', json_encode(['task_id' => $i, 'payload' => str_repeat('x', 100)]));
}

// --- 1. Consumer with advanced configuration ---

$consumer = $stream->createOrUpdateConsumer(new ConsumerConfig(
    name: 'advanced-worker',
    durable: 'advanced-worker',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    filterSubject: 'tasks.process',
    maxAckPending: 500,

    // Sampling - sends statistics for N% of messages
    sampleFrequency: '100',

    // BackOff - progressive retry (1s, 5s, 30s)
    backOff: [1.0, 5.0, 30.0],

    // Max request expires - how long a pull request can wait
    maxRequestExpires: 60.0,

    // Headers only - receives only headers without payload
    headersOnly: false,

    // Metadata
    metadata: ['team' => 'backend', 'priority' => 'high'],

    // Max deliver attempts
    maxDeliver: 5,

    // Ack wait timeout
    ackWait: 30.0,
));

echo "Consumer created: {$consumer->name()}\n";

// --- 2. Consumer Info details ---

$info = $consumer->info();
echo "Stream: {$info->stream}\n";
echo "Name: {$info->name}\n";
echo "Num pending: {$info->numPending}\n";
echo "Num ack pending: {$info->numAckPending}\n";
echo "Num redelivered: {$info->numRedelivered}\n";
echo "Num waiting: {$info->numWaiting}\n";
echo "Created: {$info->created?->format('c')}\n";
echo "Paused: " . ($info->paused ? 'yes' : 'no') . "\n";

// Cluster info (if available)
if ($info->cluster !== null) {
    echo "Cluster name: {$info->cluster->name}\n";
    echo "Cluster leader: {$info->cluster->leader}\n";
}

// --- 3. fetchBytes - fetch by size instead of message count ---

$batch = $consumer->fetchBytes(1024, new FetchOptions(timeout: 2.0)); // Max 1KB of data
$bytesReceived = 0;
$msgCount = 0;
foreach ($batch as $msg) {
    $bytesReceived += strlen($msg->data());
    $msgCount++;
    $msg->ack();
}
echo "FetchBytes: {$msgCount} messages, {$bytesReceived} bytes\n";

// --- 4. Headers Only consumer ---

$headersConsumer = $stream->createOrUpdateConsumer(new ConsumerConfig(
    name: 'headers-only',
    durable: 'headers-only',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    filterSubject: 'tasks.process',
    headersOnly: true, // Receives only headers, payload is empty
));

$batch = $headersConsumer->fetch(3, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    $meta = $msg->metadata();
    echo "Headers-only: stream_seq={$meta->streamSequence}, "
       . "consumer_seq={$meta->consumerSequence}, "
       . "pending={$meta->numPending}\n";
    $msg->ack();
}

// --- 5. Pause / Resume consumer ---

// Pause consumer for 60 seconds
$pauseUntil = new \DateTimeImmutable('+60 seconds');
$pauseResponse = $stream->pauseConsumer('advanced-worker', $pauseUntil);
echo "Consumer paused: " . ($pauseResponse->paused ? 'yes' : 'no') . "\n";
echo "Pause until: {$pauseResponse->pauseUntil?->format('c')}\n";

if ($pauseResponse->pauseRemaining !== null) {
    echo "Pause remaining: " . round($pauseResponse->pauseRemaining, 1) . "s\n";
}

// Check status through info
$pausedInfo = $consumer->info();
echo "Info paused: " . ($pausedInfo->paused ? 'yes' : 'no') . "\n";

// Resume consumer immediately
$resumeResponse = $stream->resumeConsumer('advanced-worker');
echo "Consumer resumed: " . ($resumeResponse->paused ? 'still paused' : 'active') . "\n";

// --- 6. Push Consumer CRUD ---

// Create push consumer (messages are delivered to deliverSubject)
$pushConsumer = $js->createPushConsumer('TASKS', new ConsumerConfig(
    name: 'push-worker',
    durable: 'push-worker',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    deliverSubject: 'deliver.tasks',    // Messages are sent here
    deliverGroup: 'workers',            // Queue group for load balancing
    filterSubject: 'tasks.process',
    maxDeliver: 3,
    idleHeartbeat: 15.0,                // Heartbeat every 15s
    flowControl: true,                  // Flow control for push
));

echo "Push consumer created: push-worker\n";

// Fetch push consumer by name
$existingPush = $js->pushConsumer('TASKS', 'push-worker');
echo "Push consumer fetched: push-worker\n";

// Update push consumer
$updatedPush = $js->createOrUpdatePushConsumer('TASKS', new ConsumerConfig(
    name: 'push-worker',
    durable: 'push-worker',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    deliverSubject: 'deliver.tasks',
    deliverGroup: 'workers',
    filterSubject: 'tasks.process',
    maxDeliver: 5, // Increase max deliver
    idleHeartbeat: 15.0,
    flowControl: true,
));

echo "Push consumer updated.\n";

// --- 7. List consumers ---

echo "Consumers on TASKS stream:\n";
foreach ($stream->consumerNames() as $name) {
    echo "  - {$name}\n";
}

// --- 8. Fetch remaining messages and ACK details ---

$batch = $consumer->fetch(5, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    $meta = $msg->metadata();
    echo "Msg: stream_seq={$meta->streamSequence}, "
       . "consumer_seq={$meta->consumerSequence}, "
       . "pending={$meta->numPending}, "
       . "delivered={$meta->numDelivered}\n";
    $msg->ack();
}

// Check info after processing
$finalInfo = $consumer->info();
echo "Final num_ack_pending: {$finalInfo->numAckPending}\n";
echo "Final num_pending: {$finalInfo->numPending}\n";

// --- Cleanup ---

$stream->deleteConsumer('advanced-worker');
$stream->deleteConsumer('headers-only');
$stream->deleteConsumer('push-worker');
$js->deleteStream('TASKS');
$conn->close();
