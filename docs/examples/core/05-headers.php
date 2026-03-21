<?php

/**
 * Headers - HTTP-style headers on NATS messages.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Headers;
use Nats\Message;

$conn = Connection::connect();

// --- Publish with headers ---

$headers = new Headers();
$headers->set('Content-Type', 'application/json');
$headers->set('X-Correlation-Id', 'abc-123');
$headers->add('X-Tags', 'important');
$headers->add('X-Tags', 'urgent');

$msg = new Message(
    subject: 'events.order',
    data: '{"orderId": 42}',
    headers: $headers,
);

$conn->publishMessage($msg);

// --- Subscribe and reading headers ---

$conn->subscribe('events.order', function (Message $msg) {
    echo "Subject: {$msg->subject}\n";
    echo "Data: {$msg->data}\n";

    if ($msg->hasHeaders()) {
        echo "Content-Type: {$msg->headers->get('Content-Type')}\n";
        echo "Correlation ID: {$msg->headers->get('X-Correlation-Id')}\n";
        echo "Tags: " . implode(', ', $msg->headers->values('X-Tags')) . "\n";

        // Iterate through all headers
        foreach ($msg->headers as $key => $values) {
            echo "  {$key}: " . implode(', ', $values) . "\n";
        }
    }
});

$conn->publishMessage($msg);
$conn->process(0.5);

// --- Creating headers from an array ---

$headers = new Headers([
    'X-Source' => 'php-app',
    'X-Priority' => ['high', 'critical'],
]);

echo "Wire format:\n" . $headers->toWireFormat();

$conn->close();
