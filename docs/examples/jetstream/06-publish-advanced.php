<?php

/**
 * Advanced JetStream publishing options.
 *
 * Demonstrates: publishAsync lifecycle (publishAsyncPending, publishAsyncComplete,
 * cleanupPublisher), expectLastMsgId, expectLastSequenceForSubject,
 * stallWait, msgTtl (per-message TTL), publishMessage with headers.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Headers;
use Nats\JetStream\Publish\PublishOptions;
use Nats\JetStream\Stream\StreamConfig;
use Nats\Message;

$conn = Connection::connect();
$js = $conn->jetStream();

// Setup: create stream
$stream = $js->createStream(new StreamConfig(
    name: 'EVENTS',
    subjects: ['events.>'],
    allowMsgTtl: true,    // Allow per-message TTL
));

// --- 1. Publish s message ID (dedup) ---

$ack1 = $js->publish(
    'events.order',
    json_encode(['order_id' => 1]),
    PublishOptions::msgId('order-1'),
);
echo "Publish with msg ID: seq={$ack1->sequence}, duplicate={$ack1->duplicate}\n";

// Same msg ID = duplicate (won't be stored again)
$ack2 = $js->publish(
    'events.order',
    json_encode(['order_id' => 1]),
    PublishOptions::msgId('order-1'),
);
echo "Duplicate publish: seq={$ack2->sequence}, duplicate={$ack2->duplicate}\n";

// --- 2. expectLastMsgId - ensure ordering ---

$js->publish(
    'events.payment',
    json_encode(['payment_id' => 100]),
    PublishOptions::msgId('pay-100'),
);

// Next message expects the previous one to be 'pay-100'
$ack = $js->publish(
    'events.payment',
    json_encode(['payment_id' => 101]),
    PublishOptions::msgId('pay-101'),
    PublishOptions::expectLastMsgId('pay-100'),
);
echo "Publish with expectLastMsgId: seq={$ack->sequence}\n";

// --- 3. expectLastSequencePerSubject - CAS at subject level ---

$ack = $js->publish('events.stock.AAPL', json_encode(['price' => 150.00]));
$lastSeq = $ack->sequence;

// Update only if the last sequence for subject matches expectations
$ack = $js->publish(
    'events.stock.AAPL',
    json_encode(['price' => 151.50]),
    PublishOptions::expectLastSequencePerSubject($lastSeq),
);
echo "CAS update: seq={$ack->sequence}\n";

// Failed CAS - old sequence
try {
    $js->publish(
        'events.stock.AAPL',
        json_encode(['price' => 149.00]),
        PublishOptions::expectLastSequencePerSubject($lastSeq), // Old sequence!
    );
} catch (\Throwable $e) {
    echo "CAS conflict: {$e->getMessage()}\n";
}

// --- 4. expectStream - ensure it's stored in the exact stream ---

$ack = $js->publish(
    'events.user.login',
    json_encode(['user' => 'alice']),
    PublishOptions::expectStream('EVENTS'),
);
echo "Publish with expectStream: stream={$ack->stream}\n";

// --- 5. expectLastSequence - global ordering ---

$ack = $js->publish(
    'events.system.health',
    'ok',
    PublishOptions::expectLastSequence($ack->sequence),
);
echo "Publish with expectLastSequence: seq={$ack->sequence}\n";

// --- 6. msgTtl - per-message TTL ---

// Message expires after 60 seconds (independent of stream maxAge)
$ack = $js->publish(
    'events.notification',
    json_encode(['text' => 'Temporary notification']),
    PublishOptions::msgTtl(60.0),
);
echo "Publish with TTL (60s): seq={$ack->sequence}\n";

// Short TTL - message expires quickly
$ack = $js->publish(
    'events.temp',
    'Short-lived message',
    PublishOptions::msgTtl(5.0), // 5 seconds
);
echo "Publish with short TTL (5s): seq={$ack->sequence}\n";

// --- 7. stallWait and retry options ---

$ack = $js->publish(
    'events.important',
    json_encode(['critical' => true]),
    PublishOptions::stallWait(10.0),        // Wait max 10s if buffer is full
    PublishOptions::retryAttempts(3),       // 3 retry attempts
    PublishOptions::retryWait(0.5),        // 500ms between attempts
);
echo "Publish with retry options: seq={$ack->sequence}\n";

// --- 8. publishMessage with headers ---

$headers = new Headers();
$headers->set('X-Source', 'api-gateway');
$headers->set('X-Trace-Id', 'abc-123-def');
$headers->set('X-Priority', 'high');

$msg = new Message(
    subject: 'events.audit',
    data: json_encode(['action' => 'user.delete', 'user_id' => 42]),
    headers: $headers,
);

$ack = $js->publishMessage($msg);
echo "Publish with headers: seq={$ack->sequence}\n";

// publishMessage with PublishOptions
$ack = $js->publishMessage(
    $msg,
    PublishOptions::msgId('audit-42'),
);
echo "publishMessage with msg ID: seq={$ack->sequence}\n";

// --- 9. Async publish lifecycle ---

// Send multiple messages asynchronously
$futures = [];
for ($i = 1; $i <= 10; $i++) {
    $futures[] = $js->publishAsync(
        'events.batch',
        json_encode(['batch_item' => $i]),
        PublishOptions::msgId("batch-{$i}"),
    );
}

// Check how many are pending
echo "Async pending: {$js->publishAsyncPending()}\n";
echo "Async complete: " . ($js->publishAsyncComplete() ? 'yes' : 'no') . "\n";

// Process responses
$conn->flush();
$conn->process(2.0);

// Check results
$successCount = 0;
$errorCount = 0;
foreach ($futures as $future) {
    if ($future->isComplete()) {
        $err = $future->error();
        if ($err !== null) {
            echo "Async error: {$err->getMessage()}\n";
            $errorCount++;
        } else {
            $ack = $future->ok();
            $successCount++;
        }
    }
}

echo "Async results: {$successCount} ok, {$errorCount} errors\n";
echo "All complete: " . ($js->publishAsyncComplete() ? 'yes' : 'no') . "\n";

// Clean up completed futures from memory
$js->cleanupPublisher();
echo "Publisher cleanup done, pending: {$js->publishAsyncPending()}\n";

// --- 10. Combining multiple PublishOptions ---

$ack = $js->publish(
    'events.combined',
    json_encode(['data' => 'test']),
    PublishOptions::msgId('combined-1'),
    PublishOptions::expectStream('EVENTS'),
    PublishOptions::msgTtl(300.0),         // 5 min TTL
    PublishOptions::retryAttempts(2),
);
echo "Combined options: seq={$ack->sequence}\n";

// --- Cleanup ---

$js->deleteStream('EVENTS');
$conn->close();
