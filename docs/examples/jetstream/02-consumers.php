<?php

/**
 * JetStream Consumers - reading messages from streams.
 *
 * Pull consumer: client actively requests messages (fetch).
 * Ordered consumer: guarantees order, auto-recovery.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\AckPolicy;
use Nats\Enum\DeliveryPolicy;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumeOptions;
use Nats\JetStream\Consumer\FetchOptions;
use Nats\JetStream\Consumer\OrderedConsumerConfig;
use Nats\JetStream\Message\JetStreamMessage;
use Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// Setup: create stream and publish messages
$stream = $js->createStream(new StreamConfig(
    name: 'EVENTS',
    subjects: ['events.>'],
));

for ($i = 1; $i <= 20; $i++) {
    $js->publish('events.click', json_encode(['id' => $i]));
}

// --- 1. Durable Pull Consumer ---

$consumer = $stream->createOrUpdateConsumer(new ConsumerConfig(
    name: 'click-processor',
    durable: 'click-processor',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    filterSubject: 'events.click',
    maxAckPending: 1000,
));

// --- 2. Fetch - retrieve a batch of messages ---

$batch = $consumer->fetch(5, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    echo "Fetch: {$msg->data()}\n";
    $msg->ack();
}
echo "Batch error: " . ($batch->error() ? $batch->error()->getMessage() : 'none') . "\n";

// --- 3. FetchNoWait - only available messages ---

$batch = $consumer->fetchNoWait(10);
foreach ($batch as $msg) {
    echo "NoWait: {$msg->data()}\n";
    $msg->ack();
}

// --- 4. Next - single message ---

try {
    $msg = $consumer->next(timeout: 2.0);
    echo "Next: {$msg->data()}\n";
    $msg->ack();
} catch (\Nats\TimeoutException) {
    echo "No messages.\n";
}

// --- 5. Messages - iterator (infinite loop) ---

$msgs = $consumer->messages(new ConsumeOptions(
    maxMessages: 10,
    expires: 2.0,
));

$count = 0;
foreach ($msgs as $msg) {
    echo "Iterator: {$msg->data()}\n";
    $msg->ack();
    $count++;
    if ($count >= 5) {
        // stop()  — Stops immediately, may skip buffered messages
        // drain() — Processes all buffered messages first, then stops
        $msgs->stop();
    }
}

echo "Messages context closed: " . ($msgs->isClosed() ? 'yes' : 'no') . "\n";

// --- 6. Consume - callback pattern ---

$processed = 0;
$ctx = $consumer->consume(function (JetStreamMessage $msg) use (&$processed) {
    echo "Callback: {$msg->data()}\n";
    $msg->ack();
    $processed++;
}, new ConsumeOptions(maxMessages: 5, expires: 2.0));

// ConsumeContext methods:
//   $ctx->stop()      — Stop immediately (may leave buffered messages unprocessed)
//   $ctx->drain()     — Process all buffered messages, then stop
//   $ctx->isClosed()  — Has the context stopped?
//   $ctx->lastError() — Last error during consumption (or null)
//   $ctx->info()      — Current ConsumerInfo from the server

echo "Context closed before run: " . ($ctx->isClosed() ? 'yes' : 'no') . "\n";
echo "Last error: " . ($ctx->lastError() ? $ctx->lastError()->getMessage() : 'none') . "\n";

// Fetch current consumer state from server while consuming
$consumerInfo = $ctx->info();
echo "Consumer pending: {$consumerInfo->numPending}\n";

// --- 7. ACK variants ---

$batch = $consumer->fetch(1, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    // $msg->ack();                    // Explicit ACK
    // $msg->ackSync(timeout: 5.0);    // Sync ACK (waits for confirmation)
    // $msg->nak();                    // Negative ACK (redelivery)
    // $msg->nakWithDelay(5.0);        // NAK with 5s delay
    // $msg->term();                   // Terminal - don't retry
    // $msg->termWithReason('bad data');
    // $msg->inProgress();             // Reset ack timer

    $metadata = $msg->metadata();
    echo "Stream seq: {$metadata->streamSequence}\n";
    echo "Consumer seq: {$metadata->consumerSequence}\n";
    echo "Pending: {$metadata->numPending}\n";
    $msg->ack();
}

// --- 8. Ordered Consumer (auto-recovery) ---

$ordered = $stream->orderedConsumer(new OrderedConsumerConfig(
    filterSubject: 'events.click',
    deliverPolicy: DeliveryPolicy::All,
));

$batch = $ordered->fetch(5);
foreach ($batch as $msg) {
    echo "Ordered: {$msg->data()}\n";
}

// Cleanup
$js->deleteStream('EVENTS');
$conn->close();
