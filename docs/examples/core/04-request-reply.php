<?php

/**
 * Request/Reply pattern - synchronous RPC over NATS.
 *
 * One service responds to queries, another sends requests and waits for responses.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Message;

$conn = Connection::connect();

// --- Service that responds to queries ---

$conn->subscribe('math.add', function (Message $msg) {
    $data = json_decode($msg->data, true);
    $result = ($data['a'] ?? 0) + ($data['b'] ?? 0);

    // respond() automatically sends to replyTo subject
    $msg->respond(json_encode(['result' => $result]));
});

// --- Client that sends a request ---

$reply = $conn->request('math.add', json_encode(['a' => 5, 'b' => 3]), timeout: 2.0);
$result = json_decode($reply->data, true);
echo "5 + 3 = {$result['result']}\n"; // 5 + 3 = 8

// --- Request with headers ---

$headers = new \Nats\Headers(['X-Request-Id' => 'req-001']);
$msg = new Message(
    subject: 'math.add',
    data: json_encode(['a' => 10, 'b' => 20]),
    headers: $headers,
);

$reply = $conn->requestMessage($msg, timeout: 2.0);
$result = json_decode($reply->data, true);
echo "10 + 20 = {$result['result']}\n";

$conn->close();
