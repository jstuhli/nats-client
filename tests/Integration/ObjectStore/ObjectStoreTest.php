<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\ObjectStore;

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\JetStream\JetStreamContext;
use Nats\ObjectStore\ObjectMeta;
use Nats\ObjectStore\ObjectStoreConfig;
use Nats\ObjectStore\ObjectStoreInterface;
use PHPUnit\Framework\TestCase;

final class ObjectStoreTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteObjectStore('test-obj'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteObjectStore('test-obj'); } catch (\Throwable) {}
        $this->conn->close();
    }

    private function createStore(): ObjectStoreInterface
    {
        return $this->js->createObjectStore(new ObjectStoreConfig(
            bucket: 'test-obj',
            storage: StorageType::Memory,
            maxChunkSize: 1024, // 1KB chunkovi za testiranje
        ));
    }

    public function testPutBytesAndGetBytes(): void
    {
        $store = $this->createStore();

        $info = $store->putBytes('hello.txt', 'Hello, World!');
        self::assertSame('hello.txt', $info->name);
        self::assertSame('test-obj', $info->bucket);
        self::assertSame(13, $info->size);
        self::assertGreaterThanOrEqual(1, $info->chunks);
        self::assertFalse($info->deleted);
        self::assertNotEmpty($info->digest);

        $content = $store->getBytes('hello.txt');
        self::assertSame('Hello, World!', $content);
    }

    public function testPutStringAndGetString(): void
    {
        $store = $this->createStore();

        $store->putString('config.json', '{"key": "value"}');
        $content = $store->getString('config.json');
        self::assertSame('{"key": "value"}', $content);
    }

    public function testPutWithMeta(): void
    {
        $store = $this->createStore();

        $meta = new ObjectMeta(
            name: 'report.pdf',
            description: 'Monthly report',
        );

        $info = $store->put($meta, 'PDF content here');
        self::assertSame('report.pdf', $info->name);
        self::assertSame('Monthly report', $info->description);
    }

    public function testGetStreamingResult(): void
    {
        $store = $this->createStore();
        $store->putBytes('stream-test.bin', 'streaming data');

        $result = $store->get('stream-test.bin');

        self::assertSame('stream-test.bin', $result->info()->name);
        self::assertSame(14, $result->info()->size);

        $chunk = $result->read(8);
        self::assertSame('streamin', $chunk);

        $rest = $result->readAll();
        self::assertSame('g data', $rest);

        $result->close();
    }

    public function testPutLargeObject(): void
    {
        $store = $this->createStore();

        // 5KB - s 1KB chunkovima ce biti 5 chunkova
        $data = str_repeat('X', 5120);
        $info = $store->putBytes('large.bin', $data);

        self::assertSame(5120, $info->size);
        self::assertSame(5, $info->chunks);

        $content = $store->getBytes('large.bin');
        self::assertSame(5120, strlen($content));
        self::assertSame($data, $content);
    }

    public function testPutEmptyObject(): void
    {
        $store = $this->createStore();

        $info = $store->putBytes('empty.txt', '');
        self::assertSame(0, $info->size);
        self::assertSame(1, $info->chunks);

        $content = $store->getBytes('empty.txt');
        self::assertSame('', $content);
    }

    public function testPutStream(): void
    {
        $store = $this->createStore();
        $stream = fopen('php://temp', 'w+b');
        self::assertNotFalse($stream);

        fwrite($stream, 'streamed content');
        rewind($stream);

        $info = $store->putStream('stream.txt', $stream);

        self::assertSame('stream.txt', $info->name);
        self::assertSame(16, $info->size);
        self::assertSame('streamed content', $store->getBytes('stream.txt'));

        fclose($stream);
    }

    public function testPutStreamRejectsUnreadableStream(): void
    {
        $store = $this->createStore();
        $stream = fopen('php://output', 'wb');
        self::assertNotFalse($stream);

        try {
            $this->expectException(\Nats\NatsException::class);
            $this->expectExceptionMessage('Stream for object unreadable.txt must be a readable stream resource');

            $store->putStream('unreadable.txt', $stream);
        } finally {
            fclose($stream);
        }
    }

    public function testDelete(): void
    {
        $store = $this->createStore();

        $store->putBytes('to-delete.txt', 'delete me');
        $store->delete('to-delete.txt');

        $this->expectException(\Nats\NatsException::class);
        $store->getBytes('to-delete.txt');
    }

    public function testUpdateMeta(): void
    {
        $store = $this->createStore();

        $store->putBytes('meta.txt', 'content');

        $updated = $store->updateMeta('meta.txt', new ObjectMeta(
            name: 'meta.txt',
            description: 'Updated description',
        ));

        self::assertSame('Updated description', $updated->description);
    }

    public function testList(): void
    {
        $store = $this->createStore();

        $store->putBytes('file1.txt', 'content1');
        $store->putBytes('file2.txt', 'content2');
        $store->putBytes('file3.txt', 'content3');

        $objects = $store->list();
        $names = array_map(fn($o) => $o->name, $objects);

        self::assertContains('file1.txt', $names);
        self::assertContains('file2.txt', $names);
        self::assertContains('file3.txt', $names);
    }

    public function testListExcludesDeleted(): void
    {
        $store = $this->createStore();

        $store->putBytes('keep.txt', 'keep');
        $store->putBytes('remove.txt', 'remove');
        $store->delete('remove.txt');

        $objects = $store->list();
        $names = array_map(fn($o) => $o->name, $objects);

        self::assertContains('keep.txt', $names);
        self::assertNotContains('remove.txt', $names);
    }

    public function testStatus(): void
    {
        $store = $this->createStore();
        $store->putBytes('status.txt', 'test');

        $status = $store->status();
        self::assertSame('test-obj', $status->bucket);
        self::assertGreaterThan(0, $status->size);
        self::assertGreaterThan(0, $status->objects);
        self::assertSame('memory', $status->backingStore);
    }

    public function testBucketName(): void
    {
        $store = $this->createStore();
        self::assertSame('test-obj', $store->bucket());
    }

    public function testGetNotFound(): void
    {
        $store = $this->createStore();

        $this->expectException(\Nats\NatsException::class);
        $store->getBytes('nonexistent.txt');
    }

    public function testOverwriteObject(): void
    {
        $store = $this->createStore();

        $store->putBytes('overwrite.txt', 'version 1');
        $store->putBytes('overwrite.txt', 'version 2');

        $content = $store->getBytes('overwrite.txt');
        self::assertSame('version 2', $content);
    }

    public function testDigestVerification(): void
    {
        $store = $this->createStore();
        $data = 'verify me';

        $info = $store->putBytes('digest.txt', $data);
        $expectedDigest = 'SHA-256=' . base64_encode(hash('sha256', $data, true));

        self::assertSame($expectedDigest, $info->digest);
    }

    public function testGetFile(): void
    {
        $store = $this->createStore();
        $store->putBytes('download.txt', 'file content');

        $tmpFile = tempnam(sys_get_temp_dir(), 'nats-test-');
        $store->getFile('download.txt', $tmpFile);

        self::assertSame('file content', file_get_contents($tmpFile));
        unlink($tmpFile);
    }
}
