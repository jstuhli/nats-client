<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\ObjectStore;

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\JetStream\JetStreamContext;
use Nats\NatsException;
use Nats\ObjectStore\ObjectStoreConfig;
use PHPUnit\Framework\TestCase;

final class ObjectStoreExtendedTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;
    private string $bucket;
    private string $bucket2;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        $this->bucket = 'test-objext-' . substr(uniqid(), -6);
        $this->bucket2 = 'test-objext2-' . substr(uniqid(), -6);

        try { $this->js->deleteObjectStore($this->bucket); } catch (\Throwable) {}
        try { $this->js->deleteObjectStore($this->bucket2); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteObjectStore($this->bucket); } catch (\Throwable) {}
        try { $this->js->deleteObjectStore($this->bucket2); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testGetInfo(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $store->putBytes('doc.txt', 'Hello, World!');

        // getInfo retrieves metadata without downloading content
        $info = $store->getInfo('doc.txt');
        self::assertSame('doc.txt', $info->name);
        self::assertSame($this->bucket, $info->bucket);
        self::assertSame(13, $info->size);
        self::assertGreaterThanOrEqual(1, $info->chunks);
        self::assertFalse($info->deleted);
        self::assertNotEmpty($info->digest);
    }

    public function testGetInfoNotFound(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $this->expectException(NatsException::class);
        $store->getInfo('nonexistent.txt');
    }

    public function testListShowDeleted(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $store->putBytes('keep.txt', 'keep this');
        $store->putBytes('remove.txt', 'remove this');
        $store->delete('remove.txt');

        // Without showDeleted, deleted objects should not appear
        $objects = $store->list(showDeleted: false);
        $names = array_map(fn($o) => $o->name, $objects);
        self::assertContains('keep.txt', $names);
        self::assertNotContains('remove.txt', $names);

        // With showDeleted, deleted objects should appear
        $allObjects = $store->list(showDeleted: true);
        $allNames = array_map(fn($o) => $o->name, $allObjects);
        self::assertContains('keep.txt', $allNames);
        self::assertContains('remove.txt', $allNames);

        // Verify the deleted one is marked as deleted
        $deletedObj = null;
        foreach ($allObjects as $obj) {
            if ($obj->name === 'remove.txt') {
                $deletedObj = $obj;
                break;
            }
        }
        self::assertNotNull($deletedObj);
        self::assertTrue($deletedObj->deleted);
    }

    public function testAddLink(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        // Put the target object
        $targetInfo = $store->putBytes('original.txt', 'original content');

        // Create a link to it
        $linkInfo = $store->addLink('shortcut.txt', $targetInfo);
        self::assertSame('shortcut.txt', $linkInfo->name);
        self::assertTrue($linkInfo->isLink());
        self::assertNotNull($linkInfo->link);
        self::assertSame('original.txt', $linkInfo->link->name);

        // getInfo on the link should show it's a link
        $fetchedLinkInfo = $store->getInfo('shortcut.txt');
        self::assertTrue($fetchedLinkInfo->isLink());

        // Reading via get should resolve the link and return the original content
        $content = $store->getBytes('shortcut.txt');
        self::assertSame('original content', $content);
    }

    public function testAddLinkToDeletedFails(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $info = $store->putBytes('target.txt', 'data');
        $store->delete('target.txt');

        // Get info of deleted object to pass to addLink
        $deletedInfo = $store->getInfo('target.txt', showDeleted: true);

        $this->expectException(NatsException::class);
        $store->addLink('link.txt', $deletedInfo);
    }

    public function testSeal(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $store->putBytes('sealed-file.txt', 'sealed content');

        // Seal the store
        $store->seal();

        // Verify the store is sealed
        $status = $store->status();
        self::assertTrue($status->sealed);

        // Reading should still work after sealing
        $content = $store->getBytes('sealed-file.txt');
        self::assertSame('sealed content', $content);
    }

    public function testCreateOrUpdateObjectStore(): void
    {
        // Create via createOrUpdate
        $store = $this->js->createOrUpdateObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        $store->putBytes('test.txt', 'data');

        $status = $store->status();
        self::assertNull($status->config->description);

        // Update with changed config
        $store2 = $this->js->createOrUpdateObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
            description: 'Updated store',
        ));

        $status2 = $store2->status();
        self::assertSame('Updated store', $status2->config->description);

        // Data should still be readable
        $content = $store2->getBytes('test.txt');
        self::assertSame('data', $content);
    }

    public function testObjectStoreNames(): void
    {
        $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));
        $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket2,
            storage: StorageType::Memory,
        ));

        $names = [];
        foreach ($this->js->objectStoreNames() as $name) {
            $names[] = $name;
        }

        self::assertContains($this->bucket, $names);
        self::assertContains($this->bucket2, $names);
    }

    public function testWatch(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: $this->bucket,
            storage: StorageType::Memory,
        ));

        // Put an object before starting the watch so there's something to observe
        $store->putBytes('watched.txt', 'initial');

        $watcher = $store->watch();
        $updates = $watcher->updates();

        $infos = [];
        $maxIterations = 20;
        $iteration = 0;
        foreach ($updates as $info) {
            $infos[] = $info;
            $iteration++;
            if ($iteration >= 1) {
                $watcher->stop();
                break;
            }
            if ($iteration >= $maxIterations) {
                break;
            }
        }

        self::assertGreaterThanOrEqual(1, count($infos));
        self::assertSame('watched.txt', $infos[0]->name);
    }
}
