<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\StreamConsumerLimits;
use PHPUnit\Framework\TestCase;

final class StreamConsumerLimitsTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $limits = new StreamConsumerLimits();

        self::assertNull($limits->inactiveThreshold);
        self::assertNull($limits->maxAckPending);
    }

    public function testConstructorWithValues(): void
    {
        $limits = new StreamConsumerLimits(inactiveThreshold: 30.0, maxAckPending: 500);

        self::assertSame(30.0, $limits->inactiveThreshold);
        self::assertSame(500, $limits->maxAckPending);
    }

    public function testToArrayConvertsSecondsToNanoseconds(): void
    {
        $limits = new StreamConsumerLimits(inactiveThreshold: 5.0, maxAckPending: 100);
        $array = $limits->toArray();

        self::assertSame(5_000_000_000, $array['inactive_threshold']);
        self::assertSame(100, $array['max_ack_pending']);
    }

    public function testToArrayOmitsNulls(): void
    {
        $limits = new StreamConsumerLimits();
        $array = $limits->toArray();

        self::assertEmpty($array);
    }

    public function testToArrayPartialValues(): void
    {
        $limits = new StreamConsumerLimits(maxAckPending: 200);
        $array = $limits->toArray();

        self::assertArrayNotHasKey('inactive_threshold', $array);
        self::assertSame(200, $array['max_ack_pending']);
    }

    public function testFromArrayConvertsNanosecondsToSeconds(): void
    {
        $limits = StreamConsumerLimits::fromArray([
            'inactive_threshold' => 10_000_000_000,
            'max_ack_pending' => 300,
        ]);

        self::assertSame(10.0, $limits->inactiveThreshold);
        self::assertSame(300, $limits->maxAckPending);
    }

    public function testFromArrayDefaults(): void
    {
        $limits = StreamConsumerLimits::fromArray([]);

        self::assertNull($limits->inactiveThreshold);
        self::assertNull($limits->maxAckPending);
    }

    public function testRoundTrip(): void
    {
        $original = new StreamConsumerLimits(inactiveThreshold: 60.0, maxAckPending: 1000);
        $restored = StreamConsumerLimits::fromArray($original->toArray());

        self::assertSame($original->inactiveThreshold, $restored->inactiveThreshold);
        self::assertSame($original->maxAckPending, $restored->maxAckPending);
    }
}
