<?php

/**
 * Advanced Object Store operations.
 *
 * Demonstrates: getInfo (metadata without download), addLink/addBucketLink,
 * seal, watch (ObjectWatcher), list with showDeleted, bucket management
 * (createOrUpdateObjectStore, objectStoreNames, objectStores),
 * ObjectStoreConfig with compression, ObjectMeta with metadata/chunkSize,
 * status (sealed, isCompressed).
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\Headers;
use Nats\KeyValue\WatchOptions;
use Nats\ObjectStore\ObjectMeta;
use Nats\ObjectStore\ObjectStoreConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- 1. createOrUpdateObjectStore (idempotent) ---

$store = $js->createOrUpdateObjectStore(new ObjectStoreConfig(
    bucket: 'documents',
    description: 'Document storage',
    maxBytes: 1024 * 1024 * 200,   // 200MB
    storage: StorageType::File,
    replicas: 1,
    maxChunkSize: 262144,           // 256KB chunks
    compression: true,              // S2 compression
    metadata: ['department' => 'engineering'],
));

echo "Object store created/updated: {$store->bucket()}\n";

// --- 2. Put s ObjectMeta (metadata i custom chunkSize) ---

$meta = new ObjectMeta(
    name: 'report-q1.pdf',
    description: 'Q1 Financial Report',
    headers: new Headers(['X-Author' => 'Finance Team', 'X-Version' => '2.1']),
    metadata: ['quarter' => 'Q1', 'year' => '2024', 'confidential' => 'true'],
    chunkSize: 65536,  // 64KB chunks for this object
);

$info = $store->put($meta, 'PDF content of Q1 report...');
echo "Stored: {$info->name}, size={$info->size}, chunks={$info->chunks}\n";
echo "  Description: {$info->description}\n";
echo "  Metadata: " . json_encode($info->metadata) . "\n";

// Store more objects
$store->putString('notes.txt', 'Meeting notes');
$store->putBytes('config.yaml', "database:\n  host: localhost\n  port: 5432");

// --- 3. getInfo - metadata without downloading content ---

$objectInfo = $store->getInfo('report-q1.pdf');
echo "Info (without download):\n";
echo "  Name: {$objectInfo->name}\n";
echo "  Size: {$objectInfo->size} bytes\n";
echo "  Chunks: {$objectInfo->chunks}\n";
echo "  Digest: {$objectInfo->digest}\n";
echo "  Deleted: " . ($objectInfo->deleted ? 'yes' : 'no') . "\n";
echo "  Is link: " . ($objectInfo->isLink() ? 'yes' : 'no') . "\n";

// --- 4. addLink - symbolic link to another object ---

$targetInfo = $store->getInfo('report-q1.pdf');
$linkInfo = $store->addLink('latest-report', $targetInfo);
echo "Link created: {$linkInfo->name} -> {$targetInfo->name}\n";
echo "  Link bucket: {$linkInfo->link->bucket}, name: {$linkInfo->link->name}\n";

// Reading the link automatically resolves to target
$content = $store->getString('latest-report');
echo "Link content: {$content}\n";

// --- 5. addBucketLink - link to another bucket ---

// Create another bucket
$archiveStore = $js->createObjectStore(new ObjectStoreConfig(
    bucket: 'archive',
    description: 'Document archive',
    replicas: 1,
));

$archiveStore->putString('old-report.txt', 'Old report');

// Create link from documents bucket to archive bucket
$bucketLink = $store->addBucketLink('archive-ref', $archiveStore);
echo "Bucket link created: {$bucketLink->name} -> bucket:{$bucketLink->link->bucket}\n";

// --- 6. list with showDeleted ---

// Delete one object
$store->delete('notes.txt');
echo "Object 'notes.txt' deleted.\n";

// List without deleted (default)
echo "Active objects:\n";
$objects = $store->list();
foreach ($objects as $obj) {
    echo "  - {$obj->name} (size={$obj->size}, link=" . ($obj->isLink() ? 'yes' : 'no') . ")\n";
}

// List including deleted
echo "All objects (including deleted):\n";
$allObjects = $store->list(showDeleted: true);
foreach ($allObjects as $obj) {
    $status = $obj->deleted ? '[DELETED]' : '[ACTIVE]';
    echo "  - {$status} {$obj->name}\n";
}

// --- 7. watch - tracking changes in Object Store ---

$watcher = $store->watch();

// Watcher iterates over all changes (initial + new)
// In production this goes in a separate process/fiber
// foreach ($watcher->updates() as $info) {
//     echo "Watch: {$info->name} (deleted={$info->deleted})\n";
//     $watcher->stop();
// }

echo "Object watcher created.\n";
$watcher->stop();

// Watch with options
$watcher = $store->watch(WatchOptions::updatesOnly());
echo "Watcher (updatesOnly) created.\n";
$watcher->stop();

$watcher = $store->watch(WatchOptions::ignoreDeletes());
echo "Watcher (ignoreDeletes) created.\n";
$watcher->stop();

// --- 8. Status ---

$status = $store->status();
echo "Object store status:\n";
echo "  Bucket: {$status->bucket}\n";
echo "  Size: {$status->size} bytes\n";
echo "  Objects: {$status->objects}\n";
echo "  Backing store: {$status->backingStore}\n";
echo "  Sealed: " . ($status->sealed ? 'yes' : 'no') . "\n";
echo "  Compressed: " . ($status->isCompressed ? 'yes' : 'no') . "\n";
echo "  Metadata: " . json_encode($status->metadata) . "\n";

// --- 9. Bucket management ---

// Create one more bucket
$js->createObjectStore(new ObjectStoreConfig(bucket: 'images'));

// List all Object Store bucket names
echo "Object store bucket names:\n";
foreach ($js->objectStoreNames() as $name) {
    echo "  - {$name}\n";
}

// List all Object Store bucket statuses
echo "Object store bucket statuses:\n";
foreach ($js->objectStores() as $status) {
    echo "  - {$status->bucket}: {$status->size} bytes, "
       . "{$status->objects} objects, "
       . "compressed=" . ($status->isCompressed ? 'yes' : 'no') . "\n";
}

// Fetch existing bucket
$existing = $js->objectStore('documents');
echo "Fetched bucket: {$existing->bucket()}\n";

// --- 10. seal - lock bucket (read-only) ---

// Create temporary bucket for demonstration
$sealStore = $js->createObjectStore(new ObjectStoreConfig(
    bucket: 'sealed-demo',
    description: 'Bucket for seal demo',
    replicas: 1,
));

$sealStore->putString('important.txt', 'This content will be locked');

// Seal - after this no more writing
$sealStore->seal();
echo "Bucket 'sealed-demo' sealed.\n";

$sealedStatus = $sealStore->status();
echo "Sealed status: " . ($sealedStatus->sealed ? 'yes' : 'no') . "\n";

// Attempting to write to a sealed bucket will throw an error
try {
    $sealStore->putString('new.txt', 'This will not succeed');
} catch (\Throwable $e) {
    echo "Writing to sealed bucket: {$e->getMessage()}\n";
}

// Reading still works
$content = $sealStore->getString('important.txt');
echo "Reading from sealed bucket: {$content}\n";

// --- Cleanup ---

$js->deleteObjectStore('documents');
$js->deleteObjectStore('archive');
$js->deleteObjectStore('images');
$js->deleteObjectStore('sealed-demo');
echo "All object store buckets deleted.\n";

$conn->close();
