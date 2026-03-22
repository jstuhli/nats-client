<?php

/**
 * Connection information and dynamic handlers.
 *
 * Demonstrates: rtt(), connectedAddr(), numSubscriptions(), clientId(), clientIp(),
 * authRequired(), tlsRequired(), buffered(), barrier(), stats(),
 * dynamic handler setup (setDisconnectHandler etc.)
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\ConnectionOptions;

$options = new ConnectionOptions(name: 'info-demo');

$conn = Connection::connect('nats://localhost:4222', $options);

// --- Basic connection information ---

echo "Connected URL: {$conn->connectedUrl()}\n";
echo "Connected addr (host:port): {$conn->connectedAddr()}\n";
echo "Server: {$conn->connectedServerName()} v{$conn->connectedServerVersion()}\n";
echo "Server ID: {$conn->connectedServerId()}\n";
echo "Cluster: {$conn->connectedClusterName()}\n";

// --- Client identification ---

echo "Client ID: {$conn->clientId()}\n";
echo "Client IP: {$conn->clientIp()}\n";

// --- Server requirements ---

echo "Auth required: " . ($conn->authRequired() ? 'yes' : 'no') . "\n";
echo "TLS required: " . ($conn->tlsRequired() ? 'yes' : 'no') . "\n";
echo "JetStream available: " . ($conn->jetStreamAvailable() ? 'yes' : 'no') . "\n";
echo "Headers supported: " . ($conn->headersSupported() ? 'yes' : 'no') . "\n";
echo "Max payload: {$conn->maxPayload()} bytes\n";

// --- RTT (Round Trip Time) ---

$rtt = $conn->rtt();
echo "RTT: " . round($rtt * 1000, 2) . "ms\n";

// --- Subscriptions and buffer ---

$sub1 = $conn->subscribe('test.1', function ($msg) {});
$sub2 = $conn->subscribe('test.2', function ($msg) {});
echo "Active subscriptions: {$conn->numSubscriptions()}\n";

echo "Buffered bytes: {$conn->buffered()}\n";

// --- Statistics ---

$conn->publish('test.1', 'hello');
$conn->publish('test.2', 'world');
$conn->flush();

$stats = $conn->stats();
echo "In msgs: {$stats->inMsgs}, Out msgs: {$stats->outMsgs}\n";
echo "In bytes: {$stats->inBytes}, Out bytes: {$stats->outBytes}\n";
echo "Reconnects: {$stats->reconnects}\n";

// --- Barrier - callback after server processes all previous messages ---

$conn->publish('test.1', 'before barrier');

$conn->barrier(function () {
    echo "[BARRIER] Server has processed all messages before this point\n";
});

// Process incoming messages so the barrier callback executes
$conn->process(1.0);

// --- Dynamic handler setup ---

// Handlers can also be set after connecting
$conn->setDisconnectHandler(function (Connection $c) {
    echo "[DYNAMIC] Disconnected!\n";
});

$conn->setReconnectHandler(function (Connection $c) {
    echo "[DYNAMIC] Reconnected to {$c->connectedUrl()}\n";
});

$conn->setClosedHandler(function (Connection $c) {
    echo "[DYNAMIC] Connection closed\n";
});

$conn->setErrorHandler(function (Connection $c, \Throwable $err) {
    echo "[DYNAMIC] Error: {$err->getMessage()}\n";
});

// Removing handlers (null)
$conn->setErrorHandler(null);
echo "Error handler removed.\n";

// --- Connection status ---

echo "Connected: " . ($conn->isConnected() ? 'yes' : 'no') . "\n";
echo "Closed: " . ($conn->isClosed() ? 'yes' : 'no') . "\n";
echo "Reconnecting: " . ($conn->isReconnecting() ? 'yes' : 'no') . "\n";
echo "Draining: " . ($conn->isDraining() ? 'yes' : 'no') . "\n";
echo "Status: {$conn->status()->value}\n";

// --- Last error ---

$lastErr = $conn->lastError();
echo "Last error: " . ($lastErr !== null ? $lastErr->getMessage() : 'none') . "\n";

// --- Full ServerInfo object ---
// All info from the server's initial INFO message is available as a struct.

$info = $conn->serverInfo();
if ($info !== null) {
    echo "\n--- ServerInfo ---\n";
    echo "Server name   : {$info->serverName}\n";
    echo "Server ID     : {$info->serverId}\n";
    echo "Version       : {$info->version}\n";
    echo "Protocol      : {$info->proto}\n";
    echo "Go version    : {$info->go}\n";
    echo "Host          : {$info->host}\n";
    echo "Port          : {$info->port}\n";
    echo "Max payload   : {$info->maxPayload}\n";
    echo "Cluster       : {$info->cluster}\n";
    echo "JetStream     : " . ($info->jetstream ? 'yes' : 'no') . "\n";
    echo "Headers       : " . ($info->headers ? 'yes' : 'no') . "\n";
    echo "Auth required : " . ($info->authRequired ? 'yes' : 'no') . "\n";
    echo "TLS required  : " . ($info->tlsRequired ? 'yes' : 'no') . "\n";
}

// --- Server pool ---
// servers()           — All servers the client knows about (seed + discovered).
// discoveredServers() — Only the servers announced by the cluster at runtime.

echo "\n--- Server Pool ---\n";
echo "All servers       : " . implode(', ', $conn->servers()) . "\n";
echo "Discovered servers: " . implode(', ', $conn->discoveredServers()) . "\n";

// Cleanup
$sub1->unsubscribe();
$sub2->unsubscribe();
$conn->close();
