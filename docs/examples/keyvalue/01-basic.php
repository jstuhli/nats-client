<?php

/**
 * Key-Value Store - distributed key-value store built on JetStream.
 *
 * Supports: CRUD, revisions (CAS), history, watch, TTL.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\KeyValue\KeyValueConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- Creating a bucket ---

$kv = $js->createKeyValue(new KeyValueConfig(
    bucket: 'settings',
    description: 'Application settings',
    history: 5,           // Keeps last 5 revisions
    ttl: 3600.0,          // 1h TTL
    maxValueSize: 1024,   // Max 1KB per value
    replicas: 1,
));

// --- Put / Get ---

$revision = $kv->put('theme', 'dark');
echo "Put 'theme' = 'dark', revision: {$revision}\n";

$entry = $kv->get('theme');
echo "Get 'theme': value={$entry->value}, revision={$entry->revision}\n";
echo "  bucket={$entry->bucket}, operation={$entry->operation->value}\n";
echo "  timestamp={$entry->timestamp->format('c')}\n";

// --- Create (fails if key exists) ---

try {
    $rev = $kv->create('language', 'hr');
    echo "Created 'language', revision: {$rev}\n";
} catch (\Nats\NatsException $e) {
    echo "Create failed: {$e->getMessage()}\n";
}

// --- Update (CAS - Compare And Swap) ---

$currentEntry = $kv->get('theme');
$newRev = $kv->update('theme', 'light', $currentEntry->revision);
echo "Updated 'theme' to 'light', new revision: {$newRev}\n";

// CAS failure: old revision
try {
    $kv->update('theme', 'blue', $currentEntry->revision); // Old revision!
} catch (\Nats\NatsException $e) {
    echo "CAS conflict: {$e->getMessage()}\n";
}

// --- Delete ---

$kv->delete('language');

// Delete with revision (conditional)
// $kv->delete('theme', lastRevision: $newRev);

// --- Purge (deletes all revisions of a key) ---

$kv->purge('theme');

// --- History ---

$kv->put('counter', '1');
$kv->put('counter', '2');
$kv->put('counter', '3');

$history = $kv->history('counter');
foreach ($history as $entry) {
    echo "History: rev={$entry->revision}, value={$entry->value}, op={$entry->operation->value}\n";
}

// --- List keys ---

$kv->put('a', '1');
$kv->put('b', '2');
$kv->put('c', '3');

foreach ($kv->listKeys() as $key) {
    echo "Key: {$key}\n";
}

// --- Status ---

$status = $kv->status();
echo "Bucket: {$status->bucket}\n";
echo "Values: {$status->values}\n";
echo "Bytes: {$status->bytes}\n";
echo "History: {$status->history}\n";
echo "Backing store: {$status->backingStore}\n";

// --- Cleanup ---

$js->deleteKeyValue('settings');
$conn->close();
