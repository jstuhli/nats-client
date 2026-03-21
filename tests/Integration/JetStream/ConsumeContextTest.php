<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumeOptions;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Stream\StreamConfig;
use PHPUnit\Framework\TestCase;

final class ConsumeContextTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_CONSUME_CTX'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_CONSUME_CTX'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testConsumeContextInfo(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUME_CTX',
            subjects: ['ctx.>'],
        ));

        $consumer = $this->js->createConsumer('TEST_CONSUME_CTX', new ConsumerConfig(
            name: 'ctx-consumer',
        ));

        $ctx = $consumer->consume(function ($msg): void {}, new ConsumeOptions(
            maxMessages: 1,
            expires: 0.5,
        ));

        $info = $ctx->info();
        self::assertSame('ctx-consumer', $info->config->name ?? $info->config->durable);
    }

    public function testConsumeContextLastErrorInitiallyNull(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUME_CTX',
            subjects: ['ctx.>'],
        ));

        $consumer = $this->js->createConsumer('TEST_CONSUME_CTX', new ConsumerConfig(
            name: 'ctx-lasterr',
        ));

        $ctx = $consumer->consume(function ($msg): void {}, new ConsumeOptions(
            maxMessages: 1,
            expires: 0.5,
        ));

        self::assertNull($ctx->lastError());
    }

    public function testConsumeContextStopAndDrain(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUME_CTX',
            subjects: ['ctx.>'],
        ));

        $consumer = $this->js->createConsumer('TEST_CONSUME_CTX', new ConsumerConfig(
            name: 'ctx-stopdrain',
        ));

        $ctx = $consumer->consume(function ($msg): void {}, new ConsumeOptions(
            maxMessages: 1,
            expires: 0.5,
        ));

        self::assertFalse($ctx->isClosed());
        $ctx->stop();
        self::assertTrue($ctx->isClosed());
    }

    public function testConsumeContextDrain(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUME_CTX',
            subjects: ['ctx.>'],
        ));

        $consumer = $this->js->createConsumer('TEST_CONSUME_CTX', new ConsumerConfig(
            name: 'ctx-drain',
        ));

        $received = [];
        $ctx = $consumer->consume(function ($msg) use (&$received, &$ctx): void {
            $received[] = $msg->data();
            $msg->ack();
            $ctx->drain();  // Drain after first message
        }, new ConsumeOptions(
            maxMessages: 10,
            expires: 1.0,
        ));

        $this->js->publish('ctx.drain', 'msg1');

        // Run briefly — drain should cause it to exit after processing
        $ctx->run();

        self::assertCount(1, $received);
        self::assertTrue($ctx->isClosed());
    }
}
