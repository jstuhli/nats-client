<?php

/**
 * Advanced Key-Value operations.
 *
 * Demonstrates: getRevision (specific revision), create with keyTtl (per-key TTL),
 * purge with ttl, listKeysFiltered (with patterns), watchFiltered (multiple keys),
 * bucket management (createOrUpdateKeyValue, keyValueStoreNames, keyValueStores),
 * purgeDeletes, KeyValueConfig advanced fields (mirror, sources, compression, allowMsgTtl).
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\KeyValue\KeyValueConfig;
use Nats\KeyValue\WatchOptions;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- 1. createOrUpdateKeyValue (idempotent) ---

$kv = $js->createOrUpdateKeyValue(new KeyValueConfig(
    bucket: 'cache',
    description: 'Application cache',
    history: 3,
    ttl: 7200.0,            // 2h global TTL
    maxValueSize: 4096,     // Max 4KB per value
    replicas: 1,
    compression: true,      // S2 compression
    allowMsgTtl: true,      // Allow per-key TTL
    metadata: ['env' => 'production'],
));

echo "Bucket created/updated: {$kv->bucket()}\n";

// Calling again won't produce an error
$kv = $js->createOrUpdateKeyValue(new KeyValueConfig(
    bucket: 'cache',
    description: 'Application cache (updated)',
    history: 3,
    ttl: 7200.0,
    maxValueSize: 4096,
    replicas: 1,
    compression: true,
    allowMsgTtl: true,
    metadata: ['env' => 'production'],
));
echo "Bucket updated idempotently.\n";

// --- 2. create s keyTtl (per-key TTL) ---

// Create key with its own TTL (independent of bucket's global TTL)
$rev = $kv->create('session.abc123', json_encode(['user' => 'alice', 'role' => 'admin']), keyTtl: 300.0);
echo "Session created with 5min TTL, revision: {$rev}\n";

// Normal create without per-key TTL (uses global TTL)
$rev = $kv->create('config.theme', 'dark');
echo "Config created (global TTL), revision: {$rev}\n";

// --- 3. getRevision - fetch a specific revision ---

$kv->put('counter', '1');
$kv->put('counter', '2');
$rev3 = $kv->put('counter', '3');

// Fetch previous revision
$entry = $kv->getRevision('counter', $rev3 - 1);
echo "Revision {$entry->revision}: value={$entry->value}\n";

// Fetch oldest revision
$entry = $kv->getRevision('counter', $rev3 - 2);
echo "Revision {$entry->revision}: value={$entry->value}\n";

// Current revision
$entry = $kv->getRevision('counter', $rev3);
echo "Revision {$entry->revision}: value={$entry->value} (latest)\n";

// --- 4. purge with TTL ---

$kv->put('temp.data', 'temporary data');

// Purge with TTL - purge marker stays N seconds (useful for watchers)
$kv->purge('temp.data', ttl: 30.0);
echo "Key 'temp.data' purged with 30s TTL marker\n";

// --- 5. listKeysFiltered - filtering keys with patterns ---

// Add various keys
$kv->put('user.alice.name', 'Alice');
$kv->put('user.alice.email', 'alice@example.com');
$kv->put('user.bob.name', 'Bob');
$kv->put('user.bob.email', 'bob@example.com');
$kv->put('settings.global', 'value');

// Filter keys with wildcard pattern
echo "Keys matching 'user.alice.*':\n";
foreach ($kv->listKeysFiltered('user.alice.*') as $key) {
    echo "  - {$key}\n";
}

// Multiple filters at once
echo "Keys matching 'user.*.name' or 'settings.>':\n";
foreach ($kv->listKeysFiltered('user.*.name', 'settings.>') as $key) {
    echo "  - {$key}\n";
}

// --- 6. watchFiltered - watching multiple specific keys ---

// Put data we'll be watching
$kv->put('watch.key1', 'initial-1');
$kv->put('watch.key2', 'initial-2');
$kv->put('watch.key3', 'initial-3');

// Watch only specific keys (not wildcard, but explicit list)
$watcher = $kv->watchFiltered(
    ['watch.key1', 'watch.key3'],
    WatchOptions::ignoreDeletes(),
);

// Read initial values (blocking - use in a separate process)
// foreach ($watcher->updates() as $entry) {
//     echo "Watch: key={$entry->key}, value={$entry->value}, op={$entry->operation->value}\n";
//     // Stop after initial scan for demo
//     $watcher->stop();
// }

echo "WatchFiltered created for watch.key1 and watch.key3\n";
$watcher->stop();

// --- 7. Watch with options ---

$watcher = $kv->watch('user.>', WatchOptions::updatesOnly());
// Only future updates, no initial values
echo "Watcher (updatesOnly) created\n";
$watcher->stop();

$watcher = $kv->watch('user.>', WatchOptions::ignoreDeletes());
// Ignores delete/purge operations
echo "Watcher (ignoreDeletes) created\n";
$watcher->stop();

// --- 8. purgeDeletes ---

$kv->put('to-delete-1', 'val');
$kv->put('to-delete-2', 'val');
$kv->delete('to-delete-1');
$kv->delete('to-delete-2');

// Remove all delete markers from the bucket
$kv->purgeDeletes();
echo "Delete markers cleaned.\n";

// --- 9. Bucket management ---

// Create additional buckets
$js->createKeyValue(new KeyValueConfig(bucket: 'sessions'));
$js->createKeyValue(new KeyValueConfig(bucket: 'tokens'));

// List all KV bucket names
echo "KV bucket names:\n";
foreach ($js->keyValueStoreNames() as $name) {
    echo "  - {$name}\n";
}

// List all KV bucket statuses
echo "KV bucket statuses:\n";
foreach ($js->keyValueStores() as $status) {
    echo "  - {$status->bucket}: {$status->values} values, "
       . "{$status->bytes} bytes, "
       . "compressed=" . ($status->isCompressed ? 'yes' : 'no') . "\n";
}

// Fetch existing bucket by name
$existingKv = $js->keyValue('cache');
echo "Fetched bucket: {$existingKv->bucket()}\n";

// Status bucketa
$status = $existingKv->status();
echo "Bucket status:\n";
echo "  Values: {$status->values}\n";
echo "  Bytes: {$status->bytes}\n";
echo "  History: {$status->history}\n";
echo "  TTL: {$status->ttl}s\n";
echo "  Backing store: {$status->backingStore}\n";
echo "  Compressed: " . ($status->isCompressed ? 'yes' : 'no') . "\n";
echo "  Metadata: " . json_encode($status->metadata) . "\n";

// --- Cleanup ---

$js->deleteKeyValue('cache');
$js->deleteKeyValue('sessions');
$js->deleteKeyValue('tokens');
echo "All KV buckets deleted.\n";

$conn->close();
