<?php

/**
 * Direct Get and Account Info.
 *
 * Direct Get bypasses the JetStream API layer for sub-millisecond
 * single-message retrieval — ideal for lookup-cache patterns.
 *
 * Account Info shows JetStream resource usage and limits.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\JetStream\Stream\StreamConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- 1. Account Info ---
// Inspect the current JetStream account: how many streams, consumers,
// and how much storage/memory you are using vs your limits.

$account = $js->accountInfo();

echo "=== JetStream Account ===\n";
echo "Streams   : {$account->streams}\n";
echo "Consumers : {$account->consumers}\n";
echo "Memory    : {$account->memory} bytes\n";
echo "Storage   : {$account->storage} bytes\n";

if ($account->limits !== null) {
    echo "Limits:\n";
    echo "  Max streams   : {$account->limits->maxStreams}\n";
    echo "  Max consumers : {$account->limits->maxConsumers}\n";
    echo "  Max memory    : {$account->limits->maxMemory} bytes\n";
    echo "  Max storage   : {$account->limits->maxStorage} bytes\n";
}

if ($account->domain !== null) {
    echo "Domain: {$account->domain}\n";
}

// --- 2. Setup: create a stream with allowDirect enabled ---
// allowDirect lets the server serve messages via the DIRECT.GET API
// which is faster than the standard JetStream API (no Raft round-trip).

$stream = $js->createStream(new StreamConfig(
    name: 'CACHE',
    subjects: ['cache.>'],
    allowDirect: true,
));

// Populate with some keyed data (like a cache)
$js->publish('cache.user.alice', json_encode(['name' => 'Alice', 'role' => 'admin']));
$js->publish('cache.user.bob', json_encode(['name' => 'Bob', 'role' => 'viewer']));
$js->publish('cache.user.alice', json_encode(['name' => 'Alice', 'role' => 'superadmin'])); // update
$js->publish('cache.config.theme', json_encode(['mode' => 'dark']));

echo "\n=== Direct Get ===\n";

// --- 3. Get the last message for a subject ---
// This is the "cache lookup" — returns the most recent value for the key.

$msg = $stream->directGet('cache.user.alice');
echo "Alice (latest): {$msg->data}\n";
echo "  Subject : {$msg->subject}\n";
echo "  Sequence: {$msg->sequence}\n";
echo "  Time    : {$msg->time?->format('Y-m-d H:i:s')}\n";

// --- 4. Get a message by sequence number ---
// Useful when you know the exact revision you want (like KV getRevision).

$msg = $stream->directGet('cache.user.alice', sequence: 1);
echo "\nAlice (seq 1 — original): {$msg->data}\n";

$msg = $stream->directGet('cache.user.bob');
echo "Bob: {$msg->data}\n";

// --- 5. Handle not-found ---
// Requesting a subject that has no messages returns a 404 error.

try {
    $stream->directGet('cache.user.nonexistent');
} catch (\Nats\NatsException $e) {
    echo "\nNot found: {$e->getMessage()}\n";
}

// --- 6. Comparison: directGet vs getMessage / getLastMessageForSubject ---
//
// directGet()                — Bypasses Raft. Fastest for single-message lookups.
//                              Requires allowDirect: true on the stream.
//
// getMessage($seq)           — Goes through the JetStream API. Works on any stream.
//                              Returns message by sequence number.
//
// getLastMessageForSubject() — Goes through the JetStream API. Returns the last
//                              message matching a subject.

// Standard API (works without allowDirect)
$apiMsg = $stream->getMessage(2);
echo "\ngetMessage(2): {$apiMsg->data}\n";

$apiMsg = $stream->getLastMessageForSubject('cache.config.theme');
echo "getLastMessageForSubject: {$apiMsg->data}\n";

// Cleanup
$js->deleteStream('CACHE');
$conn->close();
