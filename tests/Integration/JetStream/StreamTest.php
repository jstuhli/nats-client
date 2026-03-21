<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Enum\DiscardPolicy;
use Nats\Enum\RetentionPolicy;
use Nats\Enum\StorageType;
use Nats\JetStream\Error\JetStreamException;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamPurgeOptions;
use PHPUnit\Framework\TestCase;

final class StreamTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();

        // Cleanup streams from previous test runs
        try { $this->js->deleteStream('TEST_STREAM'); } catch (\Throwable) {}
        try { $this->js->deleteStream('TEST_STREAM2'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_STREAM'); } catch (\Throwable) {}
        try { $this->js->deleteStream('TEST_STREAM2'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testCreateStream(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            retention: RetentionPolicy::Limits,
            storage: StorageType::Memory,
            replicas: 1,
        ));

        $info = $stream->cachedInfo();
        self::assertSame('TEST_STREAM', $info->config->name);
        self::assertSame(['test.stream.>'], $info->config->subjects);
        self::assertSame(RetentionPolicy::Limits, $info->config->retention);
        self::assertSame(StorageType::Memory, $info->config->storage);
    }

    public function testStreamInfo(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->publish('test.stream.a', 'msg1');
        $this->js->publish('test.stream.b', 'msg2');

        $info = $stream->info();
        self::assertSame(2, $info->state->messages);
        self::assertGreaterThan(0, $info->state->bytes);
        self::assertSame(1, $info->state->firstSeq);
        self::assertSame(2, $info->state->lastSeq);
    }

    public function testGetMessage(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->publish('test.stream.foo', 'payload-data');

        $rawMsg = $stream->getMessage(1);
        self::assertSame('test.stream.foo', $rawMsg->subject);
        self::assertSame('payload-data', $rawMsg->data);
        self::assertSame(1, $rawMsg->sequence);
    }

    public function testGetLastMessageForSubject(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->publish('test.stream.foo', 'first');
        $this->js->publish('test.stream.foo', 'second');
        $this->js->publish('test.stream.bar', 'other');

        $rawMsg = $stream->getLastMessageForSubject('test.stream.foo');
        self::assertSame('second', $rawMsg->data);
        self::assertSame(2, $rawMsg->sequence);
    }

    public function testDeleteMessage(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->publish('test.stream.a', 'msg1');
        $this->js->publish('test.stream.b', 'msg2');
        $this->js->publish('test.stream.c', 'msg3');

        $stream->deleteMessage(2);

        $info = $stream->info();
        self::assertSame(2, $info->state->messages);
    }

    public function testPurge(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        for ($i = 0; $i < 10; $i++) {
            $this->js->publish('test.stream.data', "msg{$i}");
        }

        $purged = $stream->purge();
        self::assertSame(10, $purged);

        $info = $stream->info();
        self::assertSame(0, $info->state->messages);
    }

    public function testPurgeWithFilter(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->publish('test.stream.a', 'a1');
        $this->js->publish('test.stream.b', 'b1');
        $this->js->publish('test.stream.a', 'a2');
        $this->js->publish('test.stream.b', 'b2');

        $purged = $stream->purge(new StreamPurgeOptions(filter: 'test.stream.a'));
        self::assertSame(2, $purged);

        $info = $stream->info();
        self::assertSame(2, $info->state->messages);
    }

    public function testListStreams(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));
        $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM2',
            subjects: ['test.stream2.>'],
            storage: StorageType::Memory,
        ));

        $names = [];
        foreach ($this->js->streamNames() as $name) {
            $names[] = $name;
        }

        self::assertContains('TEST_STREAM', $names);
        self::assertContains('TEST_STREAM2', $names);
    }

    public function testDeleteStream(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        $this->js->deleteStream('TEST_STREAM');

        $this->expectException(JetStreamException::class);
        $this->js->stream('TEST_STREAM');
    }

    public function testCreateOrUpdateStreamCreatesNewStream(): void
    {
        $stream = $this->js->createOrUpdateStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
            description: 'Created via createOrUpdate',
        ));

        $info = $stream->cachedInfo();
        self::assertSame('TEST_STREAM', $info->config->name);
        self::assertSame(['test.stream.>'], $info->config->subjects);
        self::assertSame('Created via createOrUpdate', $info->config->description);
    }

    public function testCreateOrUpdateStreamUpdatesExistingStream(): void
    {
        // First, create the stream
        $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
            description: 'Original description',
        ));

        // Now createOrUpdate should update (not throw)
        $stream = $this->js->createOrUpdateStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
            description: 'Updated description',
        ));

        $info = $stream->cachedInfo();
        self::assertSame('TEST_STREAM', $info->config->name);
        self::assertSame('Updated description', $info->config->description);
    }

    public function testCreateOrUpdateStreamRethrowsNonDuplicateErrors(): void
    {
        // Create a stream that occupies certain subjects
        $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));

        // Creating a different stream with overlapping subjects should throw
        // errCode 10065 (subjects overlap), which is NOT 10058 (duplicate name)
        $this->expectException(JetStreamException::class);
        $this->js->createOrUpdateStream(new StreamConfig(
            name: 'TEST_STREAM2',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
        ));
    }

    public function testStreamWithOptions(): void
    {
        $stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_STREAM',
            subjects: ['test.stream.>'],
            storage: StorageType::Memory,
            maxMessages: 100,
            maxBytes: 1024 * 1024,
            maxAge: 3600,
            discard: DiscardPolicy::Old,
            description: 'Test stream',
        ));

        $info = $stream->cachedInfo();
        self::assertSame(100, $info->config->maxMessages);
        self::assertSame('Test stream', $info->config->description);
    }
}
