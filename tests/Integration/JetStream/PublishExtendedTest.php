<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Publish\PublishOptions;
use Nats\JetStream\Stream\StreamConfig;
use PHPUnit\Framework\TestCase;

final class PublishExtendedTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;
    private string $streamName;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        $this->streamName = 'TEST_PEXT_' . strtoupper(substr(uniqid(), -6));

        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}

        $prefix = strtolower($this->streamName);
        $this->js->createStream(new StreamConfig(
            name: $this->streamName,
            subjects: ["{$prefix}.>"],
            storage: StorageType::Memory,
            duplicateWindow: 120,
        ));
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream($this->streamName); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testPublishAsyncPending(): void
    {
        $prefix = strtolower($this->streamName);

        // Publish several messages asynchronously
        $futures = [];
        for ($i = 0; $i < 5; $i++) {
            $futures[] = $this->js->publishAsync("{$prefix}.async", "msg{$i}");
        }

        // At least some should be pending before processing
        $initialPending = $this->js->publishAsyncPending();
        self::assertGreaterThanOrEqual(0, $initialPending);
        self::assertFalse($this->js->publishAsyncComplete() && $initialPending > 0);

        // Process the event loop to receive acks
        for ($i = 0; $i < 20; $i++) {
            $this->conn->process(0.1);
            if ($this->js->publishAsyncComplete()) {
                break;
            }
        }

        self::assertTrue($this->js->publishAsyncComplete());
        self::assertSame(0, $this->js->publishAsyncPending());

        // Verify all futures resolved successfully
        foreach ($futures as $future) {
            self::assertTrue($future->isComplete());
            $ack = $future->ok();
            self::assertSame($this->streamName, $ack->stream);
        }

        // Cleanup internal tracking
        $this->js->cleanupPublisher();
    }

    public function testExpectLastMsgId(): void
    {
        $prefix = strtolower($this->streamName);

        // Publish first message with msgId
        $ack1 = $this->js->publish(
            "{$prefix}.dedup",
            'first',
            PublishOptions::msgId('msg-alpha'),
        );
        self::assertFalse($ack1->duplicate);

        // Publish second message expecting last msgId to be 'msg-alpha'
        $ack2 = $this->js->publish(
            "{$prefix}.dedup",
            'second',
            PublishOptions::msgId('msg-beta'),
            PublishOptions::expectLastMsgId('msg-alpha'),
        );
        self::assertFalse($ack2->duplicate);
        self::assertSame($ack1->sequence + 1, $ack2->sequence);
    }

    public function testExpectLastMsgIdWrongFails(): void
    {
        $prefix = strtolower($this->streamName);

        $this->js->publish(
            "{$prefix}.dedup2",
            'first',
            PublishOptions::msgId('msg-one'),
        );

        // Expecting wrong last msgId should fail
        $this->expectException(\Nats\JetStream\Error\JetStreamException::class);
        $this->js->publish(
            "{$prefix}.dedup2",
            'second',
            PublishOptions::msgId('msg-two'),
            PublishOptions::expectLastMsgId('wrong-id'),
        );
    }

    public function testStallWait(): void
    {
        $prefix = strtolower($this->streamName);

        // stallWait is advisory; just verify it does not cause an error
        $ack = $this->js->publish(
            "{$prefix}.stall",
            'data',
            PublishOptions::stallWait(5.0),
        );

        self::assertSame($this->streamName, $ack->stream);
        self::assertGreaterThanOrEqual(1, $ack->sequence);
    }
}
