<?php

/**
 * JetStream Publish - advanced publishing options.
 *
 * Deduplication, optimistic concurrency, async publish, retry.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\JetStream\Publish\PublishOptions;
use Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

$js->createStream(new StreamConfig(
    name: 'ORDERS',
    subjects: ['orders.>'],
    duplicateWindow: 120, // 2min dedupe window
));

// --- Sync publish with options ---

// Message ID for deduplication
$ack = $js->publish(
    'orders.new',
    '{"id": 1}',
    PublishOptions::msgId('order-1'),
);
echo "First publish: seq={$ack->sequence}, duplicate={$ack->duplicate}\n";

// Same msgId - server detects duplicate
$ack = $js->publish(
    'orders.new',
    '{"id": 1}',
    PublishOptions::msgId('order-1'),
);
echo "Duplicate publish: seq={$ack->sequence}, duplicate=" . ($ack->duplicate ? 'true' : 'false') . "\n";

// --- Optimistic concurrency ---

// Expect last sequence (for the entire stream)
$ack = $js->publish(
    'orders.shipped',
    '{"id": 1}',
    PublishOptions::expectLastSequence($ack->sequence),
);

// Expect last sequence per subject
$ack = $js->publish(
    'orders.new',
    '{"id": 2}',
    PublishOptions::expectLastSequencePerSubject(1),
);

// Expect stream name (verification)
$ack = $js->publish(
    'orders.new',
    '{"id": 3}',
    PublishOptions::expectStream('ORDERS'),
);

// --- Multiple options ---

$ack = $js->publish(
    'orders.new',
    '{"id": 4}',
    PublishOptions::msgId('order-4'),
    PublishOptions::expectStream('ORDERS'),
    PublishOptions::retryAttempts(3),
    PublishOptions::retryWait(0.5),
);
echo "Multi-option publish: seq={$ack->sequence}\n";

// --- Async publish ---

$future1 = $js->publishAsync('orders.new', '{"id": 5}');
$future2 = $js->publishAsync('orders.new', '{"id": 6}');
$future3 = $js->publishAsync('orders.new', '{"id": 7}');

// Do something else while ack is coming back...
$conn->process(0.5);

// Check results
if ($future1->isComplete()) {
    $ack = $future1->ok();
    echo "Async 1: seq={$ack->sequence}\n";
}

// Cleanup
$js->deleteStream('ORDERS');
$conn->close();
