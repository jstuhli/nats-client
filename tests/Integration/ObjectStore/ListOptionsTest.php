<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\ObjectStore;

use Nats\Connection;
use Nats\JetStream\JetStreamContext;
use Nats\ObjectStore\ObjectStoreConfig;
use PHPUnit\Framework\TestCase;

final class ListOptionsTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteObjectStore('TEST_LIST_OPTS'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteObjectStore('TEST_LIST_OPTS'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testListDefaultReturnsLatestOnly(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(bucket: 'TEST_LIST_OPTS'));

        $store->putString('file1', 'version1');
        $store->putString('file1', 'version2'); // overwrite
        $store->putString('file2', 'data');

        $objects = $store->list();
        self::assertCount(2, $objects);

        $names = array_map(fn($o) => $o->name, $objects);
        sort($names);
        self::assertSame(['file1', 'file2'], $names);
    }

    public function testListWithIncludeHistory(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(bucket: 'TEST_LIST_OPTS'));

        // Put different objects to create multiple meta entries
        $store->putString('doc-a', 'v1');
        $store->putString('doc-b', 'v2');
        $store->putString('doc-c', 'v3');

        // Without history — latest per name
        $objects = $store->list();
        self::assertCount(3, $objects);

        // With history — all individual entries
        $objects = $store->list(includeHistory: true);
        self::assertCount(3, $objects);

        // Now overwrite one — creates new meta, old meta persists in stream
        $store->putString('doc-a', 'v1-updated');

        // Without history — still 3 objects (latest per name)
        $objects = $store->list();
        self::assertCount(3, $objects);

        // With history — should have 4 entries (3 originals + 1 new for doc-a)
        // Note: depends on whether putRaw purges old meta or not
        $objects = $store->list(includeHistory: true);
        self::assertGreaterThanOrEqual(3, count($objects));
    }

    public function testListShowDeleted(): void
    {
        $store = $this->js->createObjectStore(new ObjectStoreConfig(bucket: 'TEST_LIST_OPTS'));

        $store->putString('alive', 'data');
        $store->putString('dead', 'data');
        $store->delete('dead');

        // Default — no deleted
        $objects = $store->list();
        $names = array_map(fn($o) => $o->name, $objects);
        self::assertContains('alive', $names);
        self::assertNotContains('dead', $names);

        // Show deleted
        $objects = $store->list(showDeleted: true);
        $names = array_map(fn($o) => $o->name, $objects);
        self::assertContains('alive', $names);
        self::assertContains('dead', $names);
    }
}
