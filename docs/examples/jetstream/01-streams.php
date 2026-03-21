<?php

/**
 * JetStream Streams - persistent message storage.
 *
 * Streams are the foundation of JetStream: they define which subjects they capture,
 * how many messages/bytes/time they retain, and how they behave at full capacity.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\DiscardPolicy;
use Nats\Enum\RetentionPolicy;
use Nats\Enum\StorageType;
use Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- Creating a stream ---

$stream = $js->createStream(new StreamConfig(
    name: 'ORDERS',
    subjects: ['orders.>'],                  // Captures all orders.* subjects
    retention: RetentionPolicy::Limits,       // Deletes when limits are reached
    maxAge: 86400,                            // Max 24h
    maxBytes: 1024 * 1024 * 100,             // Max 100MB
    maxMessages: 100_000,                     // Max 100k messages
    maxMessagesPerSubject: 10_000,           // Max 10k per subject
    storage: StorageType::File,               // Persistent on disk
    discard: DiscardPolicy::Old,              // Deletes oldest
    replicas: 1,                              // Single replica (dev)
    description: 'Order processing stream',
));

echo "Stream created: {$stream->cachedInfo()->config->name}\n";

// --- Stream info ---

$info = $stream->info();
echo "Messages: {$info->state->messages}\n";
echo "Bytes: {$info->state->bytes}\n";
echo "First seq: {$info->state->firstSeq}\n";
echo "Last seq: {$info->state->lastSeq}\n";

// --- Publish to stream ---

$ack = $js->publish('orders.new', json_encode(['id' => 1, 'item' => 'pizza']));
echo "Published: stream={$ack->stream}, seq={$ack->sequence}\n";

$ack = $js->publish('orders.new', json_encode(['id' => 2, 'item' => 'pasta']));
echo "Published: stream={$ack->stream}, seq={$ack->sequence}\n";

// --- Fetch message by sequence ---

$rawMsg = $stream->getMessage(1);
echo "Msg seq 1: subject={$rawMsg->subject}, data={$rawMsg->data}\n";

// --- Last message for subject ---

$rawMsg = $stream->getLastMessageForSubject('orders.new');
echo "Last msg: {$rawMsg->data}\n";

// --- Purge (deleting messages) ---

$purged = $stream->purge();
echo "Purged: {$purged} messages\n";

// --- List all streams ---

foreach ($js->streamNames() as $name) {
    echo "Stream: {$name}\n";
}

// --- Deleting a stream ---

$js->deleteStream('ORDERS');
echo "Stream deleted.\n";

$conn->close();
