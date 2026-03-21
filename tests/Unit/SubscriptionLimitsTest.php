<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Connection;
use Nats\Message;
use Nats\Subscription;
use PHPUnit\Framework\TestCase;

/**
 * Tests for subscription pending limits and slow consumer detection.
 * Matches Go client DefaultSubPendingMsgsLimit/DefaultSubPendingBytesLimit behavior.
 */
final class SubscriptionLimitsTest extends TestCase
{
    /**
     * Creates a Subscription with internal access for testing.
     * Uses reflection since the constructor is @internal.
     */
    private function createSubscription(?Connection $conn = null): Subscription
    {
        $ref = new \ReflectionClass(Subscription::class);
        $sub = $ref->newInstanceWithoutConstructor();

        // Set private properties via reflection
        $props = [
            'connection' => $conn ?? $this->createMockConnection(),
            'sid' => '1',
            'subject' => 'test.subject',
            'queue' => null,
            'handler' => null,
            'messageBuffer' => [],
            'delivered' => 0,
            'dropped' => 0,
            'maxMessages' => 0,
            'closed' => false,
            'draining' => false,
            'pendingMsgLimit' => 500_000,
            'pendingBytesLimit' => 67_108_864,
            'pendingBytes' => 0,
            'maxPendingMsgs' => 0,
            'maxPendingBytes' => 0,
            'closedHandler' => null,
        ];

        foreach ($props as $name => $value) {
            $prop = $ref->getProperty($name);
            $prop->setValue($sub, $value);
        }

        return $sub;
    }

    private function createMockConnection(): Connection
    {
        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        // Initialize required properties so reportSlowConsumer doesn't blow up
        $optionsProp = $ref->getProperty('options');
        $optionsProp->setValue($conn, new \Nats\ConnectionOptions());

        $lastErrorProp = $ref->getProperty('lastError');
        $lastErrorProp->setValue($conn, null);

        return $conn;
    }

    private function makeMessage(string $data = 'test'): Message
    {
        return new Message(subject: 'test.subject', data: $data);
    }

    // --- Default limits ---

    public function testDefaultPendingLimits(): void
    {
        $sub = $this->createSubscription();
        $limits = $sub->pendingLimits();

        self::assertSame(500_000, $limits['messages'], 'Default msg limit should match Go DefaultSubPendingMsgsLimit');
        self::assertSame(67_108_864, $limits['bytes'], 'Default bytes limit should match Go DefaultSubPendingBytesLimit (64MB)');
    }

    public function testSetPendingLimits(): void
    {
        $sub = $this->createSubscription();
        $sub->setPendingLimits(100, 1024);

        $limits = $sub->pendingLimits();
        self::assertSame(100, $limits['messages']);
        self::assertSame(1024, $limits['bytes']);
    }

    // --- Message limit enforcement ---

    public function testDropsMessagesWhenMsgLimitExceeded(): void
    {
        $sub = $this->createSubscription();
        $sub->setPendingLimits(3, 0); // 3 messages max, no bytes limit

        // Deliver 5 messages — first 3 accepted, last 2 dropped
        for ($i = 0; $i < 5; $i++) {
            $sub->deliver($this->makeMessage("msg-{$i}"));
        }

        self::assertSame(5, $sub->delivered(), 'All 5 should count as delivered');
        self::assertSame(2, $sub->dropped(), 'Last 2 should be dropped');
        self::assertSame(3, $sub->queuedMsgs(), 'Only 3 should be in buffer');
    }

    // --- Bytes limit enforcement ---

    public function testDropsMessagesWhenBytesLimitExceeded(): void
    {
        $sub = $this->createSubscription();
        $sub->setPendingLimits(500_000, 20); // 20 bytes max

        // Each message is 5 bytes ("msg-X"), so 4 fit in 20 bytes, 5th would exceed
        for ($i = 0; $i < 6; $i++) {
            $sub->deliver($this->makeMessage("msg-{$i}"));
        }

        // First 4 = 20 bytes. 5th would be 25 bytes > 20, dropped.
        self::assertSame(4, $sub->queuedMsgs());
        self::assertSame(2, $sub->dropped());
    }

    // --- High-water marks ---

    public function testTracksHighWaterMarks(): void
    {
        $sub = $this->createSubscription();

        $sub->deliver($this->makeMessage('aaa'));
        $sub->deliver($this->makeMessage('bbb'));
        $sub->deliver($this->makeMessage('ccc'));

        $max = $sub->maxPending();
        self::assertSame(3, $max['messages']);
        self::assertSame(9, $max['bytes']); // 3 * 3 bytes
    }

    public function testClearMaxPendingResetsHighWaterMarks(): void
    {
        $sub = $this->createSubscription();

        $sub->deliver($this->makeMessage('abc'));
        $sub->deliver($this->makeMessage('def'));

        $sub->clearMaxPending();

        $max = $sub->maxPending();
        self::assertSame(0, $max['messages']);
        self::assertSame(0, $max['bytes']);
    }

    // --- Pending tracking ---

    public function testPendingReflectsCurrentState(): void
    {
        $sub = $this->createSubscription();

        self::assertSame(0, $sub->pending()['messages']);
        self::assertSame(0, $sub->pending()['bytes']);

        $sub->deliver($this->makeMessage('hello'));

        self::assertSame(1, $sub->pending()['messages']);
        self::assertSame(5, $sub->pending()['bytes']);
    }

    // --- Slow consumer reporting ---

    public function testSlowConsumerReportedToConnection(): void
    {
        // Create a real-ish connection mock that tracks reportSlowConsumer calls
        $reported = false;
        $conn = $this->createMockConnection();

        // Use reflection to set up enough state for reportSlowConsumer to work
        $ref = new \ReflectionClass(Connection::class);

        // Set lastError property
        $lastErrorProp = $ref->getProperty('lastError');
        $lastErrorProp->setValue($conn, null);

        // Set options with error handler
        $optionsRef = new \ReflectionClass(\Nats\ConnectionOptions::class);
        $options = new \Nats\ConnectionOptions();
        $options->setOnError(function ($c, $err) use (&$reported): void {
            $reported = true;
            self::assertInstanceOf(\Nats\SlowConsumerException::class, $err);
        });

        $optionsProp = $ref->getProperty('options');
        $optionsProp->setValue($conn, $options);

        $sub = $this->createSubscription($conn);
        $sub->setPendingLimits(2, 0);

        // Fill to limit
        $sub->deliver($this->makeMessage('a'));
        $sub->deliver($this->makeMessage('b'));
        self::assertFalse($reported);

        // This one triggers slow consumer
        $sub->deliver($this->makeMessage('c'));
        self::assertTrue($reported, 'Slow consumer should have been reported');
        self::assertSame(1, $sub->dropped());
    }

    // --- Delivered count tracks all messages including dropped ---

    public function testDeliveredCountsAllMessages(): void
    {
        $sub = $this->createSubscription();
        $sub->setPendingLimits(1, 0);

        $sub->deliver($this->makeMessage('a'));
        $sub->deliver($this->makeMessage('b'));
        $sub->deliver($this->makeMessage('c'));

        self::assertSame(3, $sub->delivered(), 'Delivered should count all including dropped');
        self::assertSame(2, $sub->dropped(), 'Two should be dropped');
        self::assertSame(1, $sub->queuedMsgs(), 'Only one in buffer');
    }
}
