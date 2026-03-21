<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\KeyValue;

use Nats\Connection;
use Nats\Enum\KeyValueOperation;
use Nats\JetStream\JetStreamContext;
use Nats\KeyValue\KeyValueConfig;
use Nats\KeyValue\KeyValueInterface;
use Nats\NatsException;
use PHPUnit\Framework\TestCase;

final class KeyValueTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteKeyValue('test-kv'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteKeyValue('test-kv'); } catch (\Throwable) {}
        $this->conn->close();
    }

    private function createBucket(): KeyValueInterface
    {
        return $this->js->createKeyValue(new KeyValueConfig(
            bucket: 'test-kv',
            history: 5,
        ));
    }

    public function testPutAndGet(): void
    {
        $kv = $this->createBucket();

        $rev = $kv->put('name', 'Marko');

        self::assertGreaterThan(0, $rev);

        $entry = $kv->get('name');
        self::assertSame('name', $entry->key);
        self::assertSame('Marko', $entry->value);
        self::assertSame($rev, $entry->revision);
        self::assertSame(KeyValueOperation::Put, $entry->operation);
        self::assertSame('test-kv', $entry->bucket);
    }

    public function testGetNotFound(): void
    {
        $kv = $this->createBucket();

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Key not found');
        $kv->get('nonexistent');
    }

    public function testCreate(): void
    {
        $kv = $this->createBucket();

        $rev = $kv->create('unique', 'value');
        self::assertGreaterThan(0, $rev);

        $entry = $kv->get('unique');
        self::assertSame('value', $entry->value);
    }

    public function testCreateAlreadyExists(): void
    {
        $kv = $this->createBucket();

        $kv->create('exists', 'first');

        $this->expectException(NatsException::class);
        $kv->create('exists', 'second');
    }

    public function testUpdate(): void
    {
        $kv = $this->createBucket();

        $rev1 = $kv->put('counter', '1');
        $rev2 = $kv->update('counter', '2', $rev1);

        self::assertGreaterThan($rev1, $rev2);

        $entry = $kv->get('counter');
        self::assertSame('2', $entry->value);
    }

    public function testUpdateWrongRevision(): void
    {
        $kv = $this->createBucket();

        $kv->put('cas', 'original');
        $kv->put('cas', 'updated');

        // Pokusaj update s prvom revizijom (zastarjelo)
        $this->expectException(\Throwable::class);
        $kv->update('cas', 'conflict', 1);
    }

    public function testDelete(): void
    {
        $kv = $this->createBucket();

        $kv->put('to-delete', 'value');
        $kv->delete('to-delete');

        $this->expectException(NatsException::class);
        $kv->get('to-delete');
    }

    public function testPurge(): void
    {
        $kv = $this->createBucket();

        $kv->put('to-purge', 'v1');
        $kv->put('to-purge', 'v2');
        $kv->put('to-purge', 'v3');

        $kv->purge('to-purge');

        $this->expectException(NatsException::class);
        $kv->get('to-purge');
    }

    public function testMultipleKeys(): void
    {
        $kv = $this->createBucket();

        $kv->put('key1', 'val1');
        $kv->put('key2', 'val2');
        $kv->put('key3', 'val3');

        self::assertSame('val1', $kv->get('key1')->value);
        self::assertSame('val2', $kv->get('key2')->value);
        self::assertSame('val3', $kv->get('key3')->value);
    }

    public function testOverwriteValue(): void
    {
        $kv = $this->createBucket();

        $kv->put('mutable', 'first');
        $kv->put('mutable', 'second');
        $kv->put('mutable', 'third');

        $entry = $kv->get('mutable');
        self::assertSame('third', $entry->value);
    }

    public function testStatus(): void
    {
        $kv = $this->createBucket();

        $kv->put('a', '1');
        $kv->put('b', '2');

        $status = $kv->status();
        self::assertSame('test-kv', $status->bucket);
        self::assertGreaterThanOrEqual(2, $status->values);
        self::assertSame(5, $status->history);
        self::assertGreaterThan(0, $status->bytes);
    }

    public function testEmptyValue(): void
    {
        $kv = $this->createBucket();

        $kv->put('empty', '');
        $entry = $kv->get('empty');
        self::assertSame('', $entry->value);
    }

    public function testLargeValue(): void
    {
        $kv = $this->createBucket();

        $largeVal = str_repeat('x', 8192);
        $kv->put('large', $largeVal);

        $entry = $kv->get('large');
        self::assertSame(8192, strlen($entry->value));
    }

    public function testBucketName(): void
    {
        $kv = $this->createBucket();
        self::assertSame('test-kv', $kv->bucket());
    }

    public function testInvalidKey(): void
    {
        $kv = $this->createBucket();

        $this->expectException(NatsException::class);
        $kv->put('', 'value'); // Empty key
    }

    public function testHistory(): void
    {
        $kv = $this->createBucket();

        $kv->put('versioned', 'v1');
        $kv->put('versioned', 'v2');
        $kv->put('versioned', 'v3');

        $history = $kv->history('versioned');
        self::assertGreaterThanOrEqual(3, count($history));

        $values = array_map(fn($e) => $e->value, $history);
        self::assertContains('v1', $values);
        self::assertContains('v3', $values);
    }
}
