<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Enum\StorageType;
use Nats\Headers;
use Nats\JetStream\Error\JetStreamException;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Publish\PublishOptions;
use Nats\JetStream\Stream\StreamConfig;
use Nats\Message;
use PHPUnit\Framework\TestCase;

final class PublishTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_PUB'); } catch (\Throwable) {}

        $this->js->createStream(new StreamConfig(
            name: 'TEST_PUB',
            subjects: ['test.pub.>'],
            storage: StorageType::Memory,
            duplicateWindow: 120,
        ));
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_PUB'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testPublishBasic(): void
    {
        $ack = $this->js->publish('test.pub.basic', 'hello');

        self::assertSame('TEST_PUB', $ack->stream);
        self::assertGreaterThanOrEqual(1, $ack->sequence);
        self::assertFalse($ack->duplicate);
    }

    public function testPublishMultiple(): void
    {
        $ack1 = $this->js->publish('test.pub.multi', 'msg1');
        $ack2 = $this->js->publish('test.pub.multi', 'msg2');
        $ack3 = $this->js->publish('test.pub.multi', 'msg3');

        self::assertSame($ack1->sequence + 1, $ack2->sequence);
        self::assertSame($ack2->sequence + 1, $ack3->sequence);
    }

    public function testPublishWithMsgId(): void
    {
        $ack1 = $this->js->publish(
            'test.pub.dedup',
            'data',
            PublishOptions::msgId('unique-1'),
        );

        $ack2 = $this->js->publish(
            'test.pub.dedup',
            'data',
            PublishOptions::msgId('unique-1'),
        );

        self::assertFalse($ack1->duplicate);
        self::assertTrue($ack2->duplicate);
        self::assertSame($ack1->sequence, $ack2->sequence);
    }

    public function testPublishWithExpectStream(): void
    {
        $ack = $this->js->publish(
            'test.pub.expect',
            'data',
            PublishOptions::expectStream('TEST_PUB'),
        );

        self::assertSame('TEST_PUB', $ack->stream);
    }

    public function testPublishWithExpectLastSequence(): void
    {
        $ack1 = $this->js->publish('test.pub.seq', 'first');

        $ack2 = $this->js->publish(
            'test.pub.seq',
            'second',
            PublishOptions::expectLastSequence($ack1->sequence),
        );

        self::assertSame($ack1->sequence + 1, $ack2->sequence);
    }

    public function testPublishMessage(): void
    {
        $headers = new Headers(['X-Source' => 'test']);
        $msg = new Message(
            subject: 'test.pub.msg',
            data: 'with headers',
            headers: $headers,
        );

        $ack = $this->js->publishMessage($msg);
        self::assertSame('TEST_PUB', $ack->stream);
        self::assertGreaterThanOrEqual(1, $ack->sequence);
    }

    public function testPublishAsync(): void
    {
        $future = $this->js->publishAsync('test.pub.async', 'async data');

        self::assertFalse($future->isComplete());

        // Drive the event loop to receive the ack
        $this->conn->process(1.0);

        if ($future->isComplete()) {
            $ack = $future->ok();
            self::assertSame('TEST_PUB', $ack->stream);
        }
    }

    public function testPublishEmptyPayload(): void
    {
        $ack = $this->js->publish('test.pub.empty', '');
        self::assertGreaterThanOrEqual(1, $ack->sequence);
    }

    public function testPublishLargePayload(): void
    {
        $data = str_repeat('x', 32768); // 32KB
        $ack = $this->js->publish('test.pub.large', $data);
        self::assertGreaterThanOrEqual(1, $ack->sequence);
    }
}
