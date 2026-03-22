<?php

/**
 * Basic connection to NATS server.
 *
 * Examples of different connection methods: default, custom URL,
 * multiple servers (clustering), with options.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\ConnectionOptions;

// 1. Default connection (localhost:4222)
$conn = Connection::connect();
echo "Connected to: {$conn->connectedUrl()}\n";
echo "Server: {$conn->connectedServerName()} v{$conn->connectedServerVersion()}\n";
$conn->close();

// 2. Custom URL
$conn = Connection::connect('nats://my-nats-server:4222');
$conn->close();

// 3. Multiple servers (automatic failover)
$conn = Connection::connect(['nats://srv1:4222', 'nats://srv2:4222', 'nats://srv3:4222']);
$conn->close();

// 4. Comma-separated string
$conn = Connection::connect('nats://srv1:4222, nats://srv2:4222');
$conn->close();

// 5. With options
$options = (new ConnectionOptions())
    ->withName('my-php-app')
    ->withTimeout(5.0)
    ->withPingInterval(60.0)
    ->withMaxReconnects(10)
    ->withReconnectWait(1.0);

$conn = Connection::connect('nats://localhost:4222', $options);
echo "Max payload: {$conn->maxPayload()} bytes\n";
echo "Headers supported: " . ($conn->headersSupported() ? 'yes' : 'no') . "\n";
$conn->close();
