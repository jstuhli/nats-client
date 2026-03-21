<?php

/**
 * Advanced stream operations.
 *
 * Demonstrates: createOrUpdateStream (idempotent), streamNameBySubject,
 * secureDeleteMessage, stream info with deletedDetails and subjectFilter,
 * StreamConfig new fields (maxConsumers, allowDirect, subjectTransform, consumerLimits),
 * mirrors and sources (StreamSource with external), stream purge options (filter, sequence, keep).
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\DiscardPolicy;
use Nats\Enum\RetentionPolicy;
use Nats\Enum\StorageType;
use Nats\Enum\StoreCompression;
use Nats\JetStream\Stream\ExternalStream;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamConsumerLimits;
use Nats\JetStream\Stream\StreamPurgeOptions;
use Nats\JetStream\Stream\StreamSource;
use Nats\JetStream\Stream\SubjectTransform;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- 1. createOrUpdateStream (idempotent creation) ---

// Calling multiple times with the same configuration won't produce an error
$stream = $js->createOrUpdateStream(new StreamConfig(
    name: 'LOGS',
    subjects: ['logs.>'],
    retention: RetentionPolicy::Limits,
    maxAge: 3600,                          // 1h
    maxBytes: 1024 * 1024 * 50,           // 50MB
    maxMessages: 50_000,
    maxMessagesPerSubject: 5_000,
    maxConsumers: 10,                      // Max 10 consumera
    storage: StorageType::File,
    discard: DiscardPolicy::Old,
    replicas: 1,
    allowDirect: true,                     // Direct message fetch (faster)
    description: 'Application logs',
));

echo "Stream created/updated: {$stream->cachedInfo()->config->name}\n";
echo "Max consumers: {$stream->cachedInfo()->config->maxConsumers}\n";
echo "Allow direct: " . ($stream->cachedInfo()->config->allowDirect ? 'yes' : 'no') . "\n";

// --- 2. Subject Transform ---

$transformStream = $js->createOrUpdateStream(new StreamConfig(
    name: 'TRANSFORMED',
    subjects: ['raw.>'],
    subjectTransform: new SubjectTransform(
        source: 'raw.>',
        destination: 'processed.>',
    ),
    replicas: 1,
));

echo "Stream with transform: {$transformStream->cachedInfo()->config->name}\n";

// --- 3. Consumer Limits at stream level ---

$limitedStream = $js->createOrUpdateStream(new StreamConfig(
    name: 'LIMITED',
    subjects: ['limited.>'],
    consumerLimits: new StreamConsumerLimits(
        inactiveThreshold: 300.0,  // 5 minutes of inactivity
        maxAckPending: 500,        // Max 500 unacked messages per consumer
    ),
    replicas: 1,
));

echo "Stream with consumer limits: {$limitedStream->cachedInfo()->config->name}\n";

// --- 4. streamNameBySubject - find stream for a subject ---

$js->publish('logs.app.info', 'Test message');

$streamName = $js->streamNameBySubject('logs.app.info');
echo "Stream za 'logs.app.info': {$streamName}\n";

// --- 5. Publish messages for further examples ---

for ($i = 1; $i <= 10; $i++) {
    $js->publish("logs.app.info", "Info log {$i}");
    $js->publish("logs.app.error", "Error log {$i}");
}

// --- 6. Stream info with deletedDetails and subjectFilter ---

// Info with filtered subjects
$info = $stream->info(subjectFilter: 'logs.app.error');
echo "Messages (all): {$info->state->messages}\n";
echo "Subjects: " . json_encode($info->state->subjects ?? []) . "\n";

// Info with deleted details
$stream->deleteMessage(1); // Delete the first message
$infoWithDeleted = $stream->info(deletedDetails: true);
echo "Deleted sequences: " . json_encode($infoWithDeleted->state->deleted ?? []) . "\n";

// --- 7. secureDeleteMessage - secure deletion (data overwrite) ---

$ack = $js->publish('logs.app.secret', 'Sensitive data');
echo "Published secret: seq={$ack->sequence}\n";

// Secure deletion - data is overwritten with zeros on disk
$stream->secureDeleteMessage($ack->sequence);
echo "Message securely deleted (seq={$ack->sequence})\n";

// --- 8. Stream purge options ---

// Purge only a specific subject
$purgedByFilter = $stream->purge(new StreamPurgeOptions(
    filter: 'logs.app.error',
));
echo "Purged by filter (logs.app.error): {$purgedByFilter} messages\n";

// Publish new messages for demonstration
for ($i = 1; $i <= 10; $i++) {
    $js->publish("logs.app.debug", "Debug log {$i}");
}

// Purge - keep only the last N messages
$purgedKeep = $stream->purge(new StreamPurgeOptions(
    keep: 3,
));
echo "Purged (keep 3): {$purgedKeep} messages\n";

// Publish more messages
for ($i = 1; $i <= 5; $i++) {
    $js->publish("logs.app.trace", "Trace log {$i}");
}

// Purge everything before a specific sequence
$currentInfo = $stream->info();
$purgedBySeq = $stream->purge(new StreamPurgeOptions(
    sequence: $currentInfo->state->lastSeq - 1,
));
echo "Purged by sequence: {$purgedBySeq} messages\n";

// --- 9. Sources - aggregation from multiple streams ---

// Create source streams
$js->createOrUpdateStream(new StreamConfig(
    name: 'ORDERS-EU',
    subjects: ['orders.eu.>'],
    replicas: 1,
));

$js->createOrUpdateStream(new StreamConfig(
    name: 'ORDERS-US',
    subjects: ['orders.us.>'],
    replicas: 1,
));

// Aggregate both streams into one
$aggregated = $js->createOrUpdateStream(new StreamConfig(
    name: 'ORDERS-ALL',
    sources: [
        new StreamSource(name: 'ORDERS-EU'),
        new StreamSource(
            name: 'ORDERS-US',
            filterSubject: 'orders.us.>',
            subjectTransforms: [
                new SubjectTransform(
                    source: 'orders.us.>',
                    destination: 'orders.us.imported.>',
                ),
            ],
        ),
    ],
    replicas: 1,
));

echo "Aggregated stream: {$aggregated->cachedInfo()->config->name}\n";
echo "Sources: " . count($aggregated->cachedInfo()->config->sources) . "\n";

// --- 10. StreamConfig - advanced fields ---

$advancedStream = $js->createOrUpdateStream(new StreamConfig(
    name: 'ADVANCED',
    subjects: ['advanced.>'],
    compression: StoreCompression::S2,         // S2 compression
    duplicateWindow: 120,                       // 2 minute duplicate window
    firstSequence: 1000,                        // Starting sequence
    denyDelete: true,                           // Deny message deletion
    denyPurge: false,
    allowRollup: true,                          // Allow rollup headers
    discardNewPerSubject: true,                 // Discard new per subject
    allowMsgTtl: true,                          // Allow per-message TTL
    metadata: ['env' => 'production', 'team' => 'backend'],
    replicas: 1,
));

echo "Advanced stream: {$advancedStream->cachedInfo()->config->name}\n";
echo "Compression: {$advancedStream->cachedInfo()->config->compression->value}\n";
echo "Metadata: " . json_encode($advancedStream->cachedInfo()->config->metadata) . "\n";

// --- Cleanup ---

$js->deleteStream('LOGS');
$js->deleteStream('TRANSFORMED');
$js->deleteStream('LIMITED');
$js->deleteStream('ORDERS-EU');
$js->deleteStream('ORDERS-US');
$js->deleteStream('ORDERS-ALL');
$js->deleteStream('ADVANCED');
echo "All streams deleted.\n";

$conn->close();
