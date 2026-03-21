<?php

declare(strict_types=1);

namespace Nats\Tests\Integration;

use Nats\Connection;
use PHPUnit\Framework\TestCase;

final class SubscriptionExtendedTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testMaxPending(): void
    {
        $subject = 'test.maxpending.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        // Publish several messages
        for ($i = 0; $i < 5; $i++) {
            $this->conn->publish($subject, "msg-{$i}");
        }
        $this->conn->flush();

        // Allow messages to arrive in the sync buffer
        $deadline = microtime(true) + 2.0;
        while ($sub->queuedMsgs() < 5 && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        $maxPending = $sub->maxPending();
        self::assertArrayHasKey('messages', $maxPending);
        self::assertArrayHasKey('bytes', $maxPending);
        self::assertGreaterThanOrEqual(5, $maxPending['messages']);
        self::assertGreaterThan(0, $maxPending['bytes']);

        $sub->unsubscribe();
    }

    public function testClearMaxPending(): void
    {
        $subject = 'test.clearmaxpending.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        // Publish messages to build up high-water mark
        for ($i = 0; $i < 3; $i++) {
            $this->conn->publish($subject, "msg-{$i}");
        }
        $this->conn->flush();

        $deadline = microtime(true) + 2.0;
        while ($sub->queuedMsgs() < 3 && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        $maxBefore = $sub->maxPending();
        self::assertGreaterThanOrEqual(3, $maxBefore['messages']);

        // Clear the high-water mark
        $sub->clearMaxPending();

        $maxAfter = $sub->maxPending();
        self::assertSame(0, $maxAfter['messages']);
        self::assertSame(0, $maxAfter['bytes']);

        $sub->unsubscribe();
    }

    public function testQueuedMsgs(): void
    {
        $subject = 'test.queuedmsgs.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        self::assertSame(0, $sub->queuedMsgs());

        // Publish messages without consuming them
        for ($i = 0; $i < 4; $i++) {
            $this->conn->publish($subject, "msg-{$i}");
        }
        $this->conn->flush();

        // Wait for messages to arrive
        $deadline = microtime(true) + 2.0;
        while ($sub->queuedMsgs() < 4 && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        self::assertSame(4, $sub->queuedMsgs());

        // Consume one message
        $sub->nextMessage(1.0);
        self::assertSame(3, $sub->queuedMsgs());

        $sub->unsubscribe();
    }

    public function testSetClosedHandler(): void
    {
        $handlerCalled = false;

        $subject = 'test.closedhandler.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        $sub->setClosedHandler(function () use (&$handlerCalled): void {
            $handlerCalled = true;
        });

        $sub->unsubscribe();

        self::assertTrue($handlerCalled, 'Closed handler should have been called on unsubscribe');
    }

    public function testSetClosedHandlerOnDrain(): void
    {
        $handlerCalled = false;

        $subject = 'test.closedhandlerdrain.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        $sub->setClosedHandler(function () use (&$handlerCalled): void {
            $handlerCalled = true;
        });

        $sub->drain();

        self::assertTrue($handlerCalled, 'Closed handler should have been called on drain');
    }
}
