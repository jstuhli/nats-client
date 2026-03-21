<?php

/**
 * Object Store - storing large objects (files) in NATS.
 *
 * Objects are automatically split into chunks and reassembled when reading.
 * Supports: put (bytes/string/file/stream), get, delete, metadata.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\ObjectStore\ObjectMeta;
use Nats\ObjectStore\ObjectStoreConfig;

$conn = Connection::connect();
$js = $conn->jetStream();

// --- Creating an Object Store bucket ---

$store = $js->createObjectStore(new ObjectStoreConfig(
    bucket: 'files',
    description: 'Application file storage',
    maxBytes: 1024 * 1024 * 100,  // 100MB limit
    storage: StorageType::File,
    replicas: 1,
    maxChunkSize: 131072,          // 128KB chunks
));

// --- Put bytes ---

$info = $store->putBytes('config.json', json_encode([
    'database' => ['host' => 'localhost', 'port' => 5432],
    'cache' => ['ttl' => 3600],
]));
echo "Stored: {$info->name}, size={$info->size}, chunks={$info->chunks}\n";
echo "Digest: {$info->digest}\n";

// --- Put string ---

$info = $store->putString('readme.txt', 'This is the content of the readme file.');
echo "Stored: {$info->name}\n";

// --- Put file ---

// $info = $store->putFile('/path/to/large-file.zip');
// echo "Stored file: {$info->name}, size={$info->size}\n";

// --- Put with metadata ---

$meta = new ObjectMeta(
    name: 'report-2024.pdf',
    description: 'Annual report',
    headers: new \Nats\Headers(['X-Author' => 'Team']),
);
$info = $store->put($meta, 'PDF content here...');
echo "Stored with meta: {$info->name}, desc={$info->description}\n";

// --- Get ---

$result = $store->get('config.json');
echo "Object info: name={$result->info()->name}, size={$result->info()->size}\n";
echo "Content: {$result->readAll()}\n";
$result->close();

// --- Get bytes/string ---

$content = $store->getBytes('config.json');
echo "Bytes: {$content}\n";

$text = $store->getString('readme.txt');
echo "String: {$text}\n";

// --- Get to file ---

// $store->getFile('report-2024.pdf', '/tmp/report.pdf');

// --- Update metadata ---

$updatedInfo = $store->updateMeta('config.json', new ObjectMeta(
    name: 'config.json',
    description: 'Updated description',
));
echo "Updated meta: {$updatedInfo->description}\n";

// --- List ---

$objects = $store->list();
foreach ($objects as $obj) {
    echo "Object: {$obj->name}, size={$obj->size}, chunks={$obj->chunks}\n";
}

// --- Status ---

$status = $store->status();
echo "Bucket: {$status->bucket}\n";
echo "Size: {$status->size}\n";
echo "Objects: {$status->objects}\n";

// --- Delete ---

$store->delete('config.json');
echo "Deleted config.json\n";

// --- Cleanup ---

$js->deleteObjectStore('files');
$conn->close();
