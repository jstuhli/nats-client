<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Enum\AckPolicy;
use Nats\Enum\StorageType;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Stream\StreamConfig;
use PHPUnit\Framework\TestCase;

final class StreamExtendedTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;
    private string $streamName;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        $this->streamName = 'TEST_SEXT_' . strtoupper(substr(uniqid(), -6));

        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testCreateOrUpdateStream(): void
    {
        $prefix = strtolower($this->streamName);

        // Create initial stream
        $stream = $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
        ));

        $info = $stream->cachedInfo();
        self::assertNull($info->config->description);

        // Delete and recreate using createOrUpdateStream (which calls CREATE)
        $this->js->deleteStream($this->streamName);

        $updated = $this->js->createOrUpdateStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
            description: 'Updated description',
        ));

        $updatedInfo = $updated->cachedInfo();
        self::assertSame('Updated description', $updatedInfo->config->description);
    }

    public function testStreamNameBySubject(): void
    {
        $prefix = strtolower($this->streamName);

        $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
        ));

        $foundName = $this->js->streamNameBySubject("{$prefix}.foo.bar");
        self::assertSame($this->streamName, $foundName);
    }

    public function testSecureDeleteMessage(): void
    {
        $prefix = strtolower($this->streamName);

        $stream = $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
        ));

        $this->js->publish("{$prefix}.a", 'msg1');
        $this->js->publish("{$prefix}.b", 'msg2');
        $this->js->publish("{$prefix}.c", 'msg3');

        // Secure delete message at sequence 2
        $stream->secureDeleteMessage(2);

        $info = $stream->info();
        self::assertSame(2, $info->state->messages);
        self::assertSame(1, $info->state->numDeleted);
    }

    public function testPauseAndResumeConsumer(): void
    {
        $prefix = strtolower($this->streamName);

        $stream = $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
        ));

        $this->js->publish("{$prefix}.data", 'test');

        $consumerName = 'pause-test-' . substr(uniqid(), -6);
        $stream->createOrUpdateConsumer(new ConsumerConfig(
            name: $consumerName,
            durable: $consumerName,
            ackPolicy: AckPolicy::Explicit,
        ));

        // Pause consumer until 10 minutes in the future
        $pauseUntil = new \DateTimeImmutable('+10 minutes');

        try {
            $pauseResponse = $stream->pauseConsumer($consumerName, $pauseUntil);
            self::assertTrue($pauseResponse->paused);
            self::assertNotNull($pauseResponse->pauseUntil);

            // Resume consumer
            $resumeResponse = $stream->resumeConsumer($consumerName);
            self::assertFalse($resumeResponse->paused);
        } catch (\Nats\JetStream\Error\JetStreamException $e) {
            // Consumer pause/resume requires NATS 2.11+
            if (str_contains($e->getMessage(), 'unknown') || str_contains($e->getMessage(), 'not supported')) {
                self::markTestSkipped('Consumer pause/resume not supported on this NATS server version');
            }
            throw $e;
        }
    }

    public function testStreamInfoWithSubjectFilter(): void
    {
        $prefix = strtolower($this->streamName);

        $stream = $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
        ));

        $this->js->publish("{$prefix}.orders", 'order1');
        $this->js->publish("{$prefix}.orders", 'order2');
        $this->js->publish("{$prefix}.payments", 'pay1');
        $this->js->publish("{$prefix}.shipments", 'ship1');

        // Get info with subject filter
        $info = $stream->info(subjectFilter: "{$prefix}.orders");
        self::assertSame(4, $info->state->messages);
        // The numSubjects in the filtered response should reflect total subjects
        self::assertGreaterThanOrEqual(1, $info->state->numSubjects);
    }

    public function testStreamConfigNewFields(): void
    {
        $prefix = strtolower($this->streamName);

        $stream = $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
            maxConsumers: 10,
            allowDirect: true,
            description: 'Stream with extended config',
        ));

        $info = $stream->cachedInfo();
        self::assertSame(10, $info->config->maxConsumers);
        self::assertTrue($info->config->allowDirect);
        self::assertSame('Stream with extended config', $info->config->description);
    }
}
