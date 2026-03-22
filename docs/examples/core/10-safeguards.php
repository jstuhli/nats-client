<?php

/**
 * Client-side safeguards.
 *
 * Demonstrates: max-payload enforcement, subject validation,
 * slow consumer detection via pending limits, and header support checks.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\BadSubjectException;
use Nats\Connection;
use Nats\ConnectionOptions;
use Nats\Headers;
use Nats\MaxPayloadException;
use Nats\Message;
use Nats\SlowConsumerException;

// --- 1. Max payload enforcement ---
// The client knows the server limit (sent in the INFO message at connect).
// It rejects oversized publishes *before* writing a single byte to the wire.

$conn = Connection::connect();

$maxPayload = $conn->maxPayload();
echo "Server max payload: {$maxPayload} bytes\n";

// Safe publish — within the limit
$conn->publish('safe.msg', str_repeat('A', 1024));
echo "Published 1 KB message OK\n";

// Oversized publish — caught immediately
try {
    $conn->publish('safe.msg', str_repeat('B', $maxPayload + 1));
} catch (MaxPayloadException $e) {
    echo "Blocked: {$e->getMessage()}\n";
}

// Headers count towards the payload size.
// A small body plus large headers can still exceed the limit.
$headers = new Headers(['X-Trace-Id' => str_repeat('t', 512)]);
$msg = new Message('safe.msg', str_repeat('C', $maxPayload - 256), headers: $headers);
try {
    $conn->publishMessage($msg);
} catch (MaxPayloadException $e) {
    echo "Blocked (headers + body): {$e->getMessage()}\n";
}

$conn->close();

// --- 2. Subject validation ---
// Subjects must not be empty and must not contain spaces, tabs,
// carriage returns or newlines. This prevents protocol injection.

$conn = Connection::connect();

$badSubjects = [
    ''                => 'empty string',
    'has space'       => 'contains space',
    "has\ttab"        => 'contains tab',
    "has\nnewline"    => 'contains newline',
    "has\rreturn"     => 'contains carriage return',
];

foreach ($badSubjects as $subject => $description) {
    try {
        $conn->publish($subject, 'test');
        echo "BUG: published to '{$description}' subject!\n";
    } catch (BadSubjectException $e) {
        echo "Rejected ({$description}): {$e->getMessage()}\n";
    }
}

// Valid subjects (wildcards are fine, dots are fine)
$validSubjects = ['orders.us.new', 'events.*', 'logs.>'];
foreach ($validSubjects as $subject) {
    // subscribe accepts wildcards
    $sub = $conn->subscribe($subject, function () {});
    $sub->unsubscribe();
    echo "Valid: {$subject}\n";
}

$conn->close();

// --- 3. Skip subject validation for hot paths ---
// If you have already validated your subjects upstream,
// you can trade the safety check for throughput.

$opts = (new ConnectionOptions())->withSkipSubjectValidation();
$conn = Connection::connect('nats://localhost:4222', $opts);

// This won't throw — validation is disabled
// (but the server will still reject truly broken subjects)
$conn->publish('fast.path', 'data');
echo "\nPublished with validation skipped\n";
$conn->close();

// --- 4. Slow consumer detection ---
// When a sync subscriber's internal queue fills up, the client
// delivers a SlowConsumerException through the error callback
// and starts counting dropped messages.

$slowConsumerDetected = false;

$opts = (new ConnectionOptions())
    ->withOnError(function (Connection $conn, \Throwable $err) use (&$slowConsumerDetected) {
        if ($err instanceof SlowConsumerException) {
            $slowConsumerDetected = true;
            echo "Slow consumer detected: {$err->getMessage()}\n";
        }
    });

$conn = Connection::connect('nats://localhost:4222', $opts);
$sub = $conn->subscribeSync('load.test');

// Set very tight pending limits: 5 messages max, 1 KB bytes max
$sub->setPendingLimits(msgLimit: 5, bytesLimit: 1024);
echo "\nPending limits: " . json_encode($sub->pendingLimits()) . "\n";

// Flood: publish more messages than the buffer can hold
for ($i = 0; $i < 50; $i++) {
    $conn->publish('load.test', "payload-{$i}");
}
$conn->flush();

// Process incoming data so the client delivers the SlowConsumerException
$conn->process(0.5);

// Check metrics
$pending = $sub->pending();
echo "Pending: {$pending['messages']} msgs, {$pending['bytes']} bytes\n";
echo "Delivered: {$sub->delivered()}\n";
echo "Dropped: {$sub->dropped()}\n";
echo "Max pending: " . json_encode($sub->maxPending()) . "\n";

$sub->unsubscribe();
$conn->close();

// --- 5. Header support check ---

$conn = Connection::connect();
echo "\nHeaders supported: " . ($conn->headersSupported() ? 'yes' : 'no') . "\n";
$conn->close();
