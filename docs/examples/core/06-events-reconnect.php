<?php

/**
 * Event handlers and reconnect logic.
 *
 * Demonstrates: onConnect, onDisconnect, onReconnect, onClose, onError,
 * reconnect configuration, drain.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\ConnectionOptions;

$options = (new ConnectionOptions())
    ->name('resilient-app')

    // Reconnect configuration
    ->maxReconnects(-1)          // Infinite attempts
    ->reconnectWait(2.0)         // 2s between attempts
    ->reconnectJitter(0.5, 1.0)  // Random jitter
    ->reconnectBufferSize(8 * 1024 * 1024) // 8MB buffer for messages during reconnect

    // Event handlers
    ->onConnect(function (Connection $conn) {
        echo "[EVENT] Connected to {$conn->connectedUrl()}\n";
    })
    ->onDisconnect(function (Connection $conn) {
        echo "[EVENT] Disconnected!\n";
    })
    ->onReconnect(function (Connection $conn) {
        $stats = $conn->stats();
        echo "[EVENT] Reconnected to {$conn->connectedUrl()} (total reconnects: {$stats->reconnects})\n";
    })
    ->onClose(function (Connection $conn) {
        echo "[EVENT] Connection closed\n";
    })
    ->onError(function (Connection $conn, \Throwable $err) {
        echo "[ERROR] {$err->getMessage()}\n";
    })
    ->onLameDuckMode(function (Connection $conn) {
        echo "[EVENT] Server entering lame duck mode - preparing for shutdown\n";
    });

$conn = Connection::connect('nats://localhost:4222', $options);

// Statistics
$stats = $conn->stats();
echo "In msgs: {$stats->inMsgs}, Out msgs: {$stats->outMsgs}\n";
echo "In bytes: {$stats->inBytes}, Out bytes: {$stats->outBytes}\n";

// Flush - ensures all messages have been sent
$conn->flush(timeout: 2.0);

$conn->close();

// --- Custom reconnect delay (exponential backoff) ---
// Instead of the fixed reconnectWait, provide a closure that receives
// the attempt number and returns how many seconds to wait.

$backoffOptions = (new ConnectionOptions())
    ->name('backoff-demo')
    ->customReconnectDelay(function (int $attempts): float {
        // Exponential backoff: 0.5s, 1s, 2s, 4s ... capped at 30s
        return min(0.5 * (2 ** ($attempts - 1)), 30.0);
    });

// --- Retry on failed initial connect ---
// Normally, connect() throws immediately if the server is down.
// With retryOnFailedConnect the client keeps trying in the background.

$retryOptions = (new ConnectionOptions())
    ->retryOnFailedConnect()
    ->maxReconnects(10);

// --- No reconnect ---
// Disable reconnection entirely — the connection closes on first disconnect.

$noReconnectOptions = (new ConnectionOptions())
    ->noReconnect();

// --- Discovered servers handler ---
// In a cluster, the server may advertise new nodes. This handler fires
// when the client learns about servers it didn't know at connect time.

$clusterOptions = (new ConnectionOptions())
    ->onDiscoveredServers(function (Connection $conn) {
        echo "[DISCOVERED] New servers: " . implode(', ', $conn->discoveredServers()) . "\n";
    });

// --- Force reconnect ---
// Programmatically trigger a reconnect (e.g. after a configuration change).

// $conn->forceReconnect();
