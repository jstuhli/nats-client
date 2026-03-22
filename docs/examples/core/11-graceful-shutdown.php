<?php

/**
 * Drain and graceful shutdown patterns.
 *
 * Demonstrates: connection drain vs close, subscription drain,
 * lame duck mode, drain timeout, and signal handling for
 * clean process termination.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\ConnectionOptions;
use Nats\Enum\ConnectionStatus;

// ==========================================================================
// DRAIN vs CLOSE
//
// close()  — Hard stop. Drops the TCP socket immediately. Pending messages
//            in the outbound buffer may be lost. Subscriptions stop at once.
//
// drain()  — Graceful stop.
//            1. Moves status to DrainingSubscriptions.
//            2. Drains every subscription (unsubscribes, flushes pending).
//            3. Moves status to DrainingPublications.
//            4. Flushes the outbound buffer.
//            5. Calls close().
//
// In production you almost always want drain().
// ==========================================================================

// --- 1. Basic drain ---

$conn = Connection::connect();
$received = 0;

$sub = $conn->subscribe('work.items', function ($msg) use (&$received) {
    $received++;
    // Simulate processing
});

$conn->publish('work.items', 'job-1');
$conn->publish('work.items', 'job-2');
$conn->flush();
$conn->process(0.2);

echo "Received before drain: {$received}\n";

// drain() ensures in-flight messages are processed before closing.
$conn->drain();

echo "Status after drain: {$conn->status()->value}\n"; // ConnectionStatus::Closed

// --- 2. Subscription-level drain ---
// You can drain a single subscription while keeping the connection alive.

$conn = Connection::connect();
$processed = [];

$sub1 = $conn->subscribe('topic.a', function ($msg) use (&$processed) {
    $processed[] = 'a:' . $msg->data;
});
$sub2 = $conn->subscribe('topic.b', function ($msg) use (&$processed) {
    $processed[] = 'b:' . $msg->data;
});

$conn->publish('topic.a', '1');
$conn->publish('topic.b', '2');
$conn->flush();
$conn->process(0.2);

// Drain only topic.a — it processes remaining messages, then unsubscribes.
$sub1->drain();
echo "Sub A valid after drain: " . ($sub1->isValid() ? 'yes' : 'no') . "\n"; // no

// topic.b still works:
$conn->publish('topic.b', '3');
$conn->flush();
$conn->process(0.2);
echo "Processed: " . implode(', ', $processed) . "\n";

$conn->close();

// --- 3. Drain timeout ---
// If subscriptions take too long to drain, the timeout kicks in
// and the connection closes anyway.

$opts = (new ConnectionOptions())
    ->withDrainTimeout(5.0); // Max 5 seconds to drain

$conn = Connection::connect('nats://localhost:4222', $opts);
$conn->subscribe('slow.worker', function ($msg) {
    // Even a slow handler will be interrupted by the drain timeout
});
$conn->drain();

// --- 4. Lame duck mode ---
// When the server is about to shut down (e.g. `nats-server --signal ldm`),
// it sends a lame-duck-mode notification. The recommended response
// is to drain and reconnect to a different server.

$opts = (new ConnectionOptions())
    ->withOnLameDuckMode(function (Connection $conn) {
        echo "[LAME DUCK] Server is shutting down — draining ...\n";
        $conn->drain();
    })
    ->withOnClose(function (Connection $conn) {
        echo "[CLOSED] Goodbye.\n";
    });

$conn = Connection::connect('nats://localhost:4222', $opts);
// In real life the lame duck callback fires automatically when the
// server announces shutdown. No action needed from your side.
$conn->close();

// --- 5. Status transitions during drain ---
// Useful for health-check endpoints or readiness probes.

$statusLog = [];

$opts = (new ConnectionOptions())
    ->withName('status-demo');

$conn = Connection::connect('nats://localhost:4222', $opts);

echo "\nBefore drain:\n";
echo "  isConnected: " . ($conn->isConnected() ? 'yes' : 'no') . "\n";
echo "  isDraining:  " . ($conn->isDraining() ? 'yes' : 'no') . "\n";
echo "  status:      {$conn->status()->value}\n";

// drain() transitions: Connected → DrainingSubscriptions → DrainingPublications → Closed
$conn->drain();

echo "After drain:\n";
echo "  isConnected: " . ($conn->isConnected() ? 'yes' : 'no') . "\n";
echo "  isClosed:    " . ($conn->isClosed() ? 'yes' : 'no') . "\n";
echo "  status:      {$conn->status()->value}\n";

// --- 6. Signal handling (requires pcntl) ---
// A common production pattern: trap SIGTERM/SIGINT and drain.

/*
$conn = Connection::connect();

if (function_exists('pcntl_signal')) {
    pcntl_signal(SIGTERM, function () use ($conn) {
        echo "Received SIGTERM — draining ...\n";
        $conn->drain();
    });

    pcntl_signal(SIGINT, function () use ($conn) {
        echo "Received SIGINT — draining ...\n";
        $conn->drain();
    });
}

// Your main loop:
$conn->subscribe('jobs.>', function ($msg) {
    // process work ...
    echo "Processing: {$msg->data}\n";
});

// Blocking wait — will exit when drain() is called from a signal handler.
// $conn->wait();
*/
