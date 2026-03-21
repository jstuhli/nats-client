<?php

/**
 * Advanced subscription options.
 *
 * Demonstrates: subscribeSync with maxPending/clearMaxPending, queuedMsgs(),
 * setClosedHandler, autoUnsubscribe, setPendingLimits, pending(), delivered(), dropped().
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Message;

$conn = Connection::connect();

// --- 1. Sync subscription with pending limits ---

$sub = $conn->subscribeSync('demo.pending');

// Set limits for pending messages (max messages, max bytes)
$sub->setPendingLimits(msgLimit: 100, bytesLimit: 1024 * 1024); // 100 messages, 1MB

// Publish a few messages
for ($i = 1; $i <= 5; $i++) {
    $conn->publish('demo.pending', "message-{$i}");
}
$conn->flush();

// Process incoming messages to fill the buffer
$conn->process(0.5);

// Check how many messages are waiting in the buffer
echo "Queued msgs: {$sub->queuedMsgs()}\n";

$pending = $sub->pending();
echo "Pending: {$pending['messages']} messages, {$pending['bytes']} bytes\n";

// Read messages
while ($sub->queuedMsgs() > 0) {
    $msg = $sub->nextMessage(1.0);
    echo "Received: {$msg->data}\n";
}

echo "Delivered: {$sub->delivered()}\n";
echo "Dropped: {$sub->dropped()}\n";

// --- 2. Max pending (high-water mark) ---

// Publish more messages to track the high-water mark
for ($i = 1; $i <= 10; $i++) {
    $conn->publish('demo.pending', "batch-{$i}");
}
$conn->flush();
$conn->process(0.5);

$maxPending = $sub->maxPending();
echo "Max pending (high-water): {$maxPending['messages']} messages, {$maxPending['bytes']} bytes\n";

// Reset high-water mark
$sub->clearMaxPending();
$maxPendingAfterClear = $sub->maxPending();
echo "Max pending after clear: {$maxPendingAfterClear['messages']} messages\n";

// Drain buffer
while ($sub->queuedMsgs() > 0) {
    $sub->nextMessage(1.0);
}

$sub->unsubscribe();

// --- 3. AutoUnsubscribe - automatically unsubscribe after N messages ---

$autoSub = $conn->subscribeSync('demo.auto');
$autoSub->autoUnsubscribe(3); // Receive max 3 messages

for ($i = 1; $i <= 5; $i++) {
    $conn->publish('demo.auto', "auto-{$i}");
}
$conn->flush();
$conn->process(0.5);

// Only the first 3 messages will be received
$count = 0;
while (true) {
    try {
        $msg = $autoSub->nextMessage(0.5);
        $count++;
        echo "Auto [{$count}]: {$msg->data}\n";
    } catch (\Nats\TimeoutException) {
        break;
    }
}
echo "Auto received: {$count} messages\n";
echo "Subscription valid: " . ($autoSub->isValid() ? 'yes' : 'no') . "\n";

// --- 4. setClosedHandler - callback when subscription closes ---

$closedSub = $conn->subscribeSync('demo.closed');
$closedSub->setClosedHandler(function () {
    echo "[CLOSED] Subscription 'demo.closed' has been closed!\n";
});

// Unsubscribe calls closedHandler
$closedSub->unsubscribe();

// --- 5. setClosedHandler s drain ---

$drainSub = $conn->subscribeSync('demo.drain');
$drainSub->setClosedHandler(function () {
    echo "[CLOSED] Subscription 'demo.drain' closed after drain\n";
});

// Drain also calls closedHandler
$drainSub->drain();

// --- 6. Async subscription with delivered/dropped tracking ---

$delivered = 0;
$asyncSub = $conn->subscribe('demo.async', function (Message $msg) use (&$delivered) {
    $delivered++;
});

for ($i = 1; $i <= 10; $i++) {
    $conn->publish('demo.async', "async-{$i}");
}
$conn->flush();
$conn->process(0.5);

echo "Async delivered (internal): {$asyncSub->delivered()}\n";
echo "Async dropped: {$asyncSub->dropped()}\n";

$asyncSub->unsubscribe();

// --- 7. Queue subscription ---

$q1 = $conn->queueSubscribeSync('demo.queue', 'workers');
$q2 = $conn->queueSubscribeSync('demo.queue', 'workers');

for ($i = 1; $i <= 4; $i++) {
    $conn->publish('demo.queue', "job-{$i}");
}
$conn->flush();
$conn->process(0.5);

echo "Queue sub 1 queued: {$q1->queuedMsgs()}\n";
echo "Queue sub 2 queued: {$q2->queuedMsgs()}\n";
echo "Queue sub 1 subject: {$q1->subject()}, queue: {$q1->queue()}\n";

$q1->unsubscribe();
$q2->unsubscribe();

$conn->close();
