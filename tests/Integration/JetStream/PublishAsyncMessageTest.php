<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Headers;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Stream\StreamConfig;
use Nats\Message;
use PHPUnit\Framework\TestCase;

final class PublishAsyncMessageTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_ASYNC_MSG'); } catch (\Throwable) {}
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_ASYNC_MSG'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testPublishAsyncMessageWithHeaders(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_ASYNC_MSG',
            subjects: ['asyncmsg.>'],
        ));

        $msg = new Message(
            subject: 'asyncmsg.test',
            data: 'async-payload',
            headers: new Headers(['X-Custom' => 'value123']),
        );

        $future = $this->js->publishAsyncMessage($msg);

        // Process to get the ack
        $deadline = microtime(true) + 5.0;
        while (!$future->isComplete() && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        self::assertTrue($future->isComplete(), 'Future should be complete');
        $ack = $future->ok();
        self::assertSame('TEST_ASYNC_MSG', $ack->stream);
        self::assertGreaterThan(0, $ack->sequence);
    }

    public function testPublishAsyncMessageWithoutHeaders(): void
    {
        $this->js->createStream(new StreamConfig(
            name: 'TEST_ASYNC_MSG',
            subjects: ['asyncmsg.>'],
        ));

        $msg = new Message(
            subject: 'asyncmsg.plain',
            data: 'no-headers',
        );

        $future = $this->js->publishAsyncMessage($msg);

        $deadline = microtime(true) + 5.0;
        while (!$future->isComplete() && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        self::assertTrue($future->isComplete());
        self::assertGreaterThan(0, $future->ok()->sequence);
    }
}
