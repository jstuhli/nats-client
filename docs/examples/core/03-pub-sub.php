<?php

/**
 * Publish/Subscribe - basic messaging pattern.
 *
 * Shows: publish, subscribe (async callback), subscribe sync,
 * wildcards, queue groups.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Message;

$conn = Connection::connect();

// --- Async subscribe (callback) ---

$conn->subscribe('greetings', function (Message $msg) {
    echo "Received on '{$msg->subject}': {$msg->data}\n";
});

$conn->publish('greetings', 'Hello NATS!');
$conn->publish('greetings', 'Hello from PHP!');

// Process messages (non-blocking)
$conn->process(0.5);

// --- Sync subscribe ---

$sub = $conn->subscribeSync('orders.>');
$conn->publish('orders.new', '{"id": 1, "item": "pizza"}');
$conn->publish('orders.shipped', '{"id": 1}');

$msg1 = $sub->nextMessage(1.0);
echo "Sync message 1: {$msg1->data}\n";

$msg2 = $sub->nextMessage(1.0);
echo "Sync message 2: {$msg2->data}\n";

$sub->unsubscribe();

// --- Wildcard subscriptions ---

// '*' - single token
$conn->subscribe('events.*.created', function (Message $msg) {
    echo "Created event: {$msg->subject}\n";
});

// '>' - everything below
$conn->subscribe('logs.>', function (Message $msg) {
    echo "Log: {$msg->subject} -> {$msg->data}\n";
});

$conn->publish('events.user.created', 'user 42');
$conn->publish('logs.app.info', 'App started');
$conn->publish('logs.db.error', 'Connection lost');
$conn->process(0.5);

// --- Queue groups (load balancing) ---

// Three workers in the same queue group - only one receives the message
$conn->queueSubscribe('tasks', 'workers', function (Message $msg) {
    echo "Worker A: {$msg->data}\n";
});
$conn->queueSubscribe('tasks', 'workers', function (Message $msg) {
    echo "Worker B: {$msg->data}\n";
});
$conn->queueSubscribe('tasks', 'workers', function (Message $msg) {
    echo "Worker C: {$msg->data}\n";
});

for ($i = 1; $i <= 5; $i++) {
    $conn->publish('tasks', "Task #{$i}");
}
$conn->process(0.5);

// --- Auto-unsubscribe ---

$sub = $conn->subscribeSync('limited');
$sub->autoUnsubscribe(3); // Receive max 3 messages

for ($i = 1; $i <= 5; $i++) {
    $conn->publish('limited', "msg {$i}");
}

$conn->close();
