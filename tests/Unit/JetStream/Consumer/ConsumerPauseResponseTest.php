<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Consumer;

use Nats\JetStream\Consumer\ConsumerPauseResponse;
use PHPUnit\Framework\TestCase;

final class ConsumerPauseResponseTest extends TestCase
{
    public function testFromArrayPaused(): void
    {
        $data = [
            'paused' => true,
            'pause_until' => '2026-04-01T12:00:00+00:00',
            'pause_remaining' => 60_000_000_000,
        ];

        $response = ConsumerPauseResponse::fromArray($data);

        self::assertTrue($response->paused);
        self::assertInstanceOf(\DateTimeImmutable::class, $response->pauseUntil);
        self::assertSame('2026-04-01', $response->pauseUntil->format('Y-m-d'));
        self::assertSame(60.0, $response->pauseRemaining);
    }

    public function testFromArrayNotPaused(): void
    {
        $response = ConsumerPauseResponse::fromArray(['paused' => false]);

        self::assertFalse($response->paused);
        self::assertNull($response->pauseUntil);
        self::assertNull($response->pauseRemaining);
    }

    public function testFromArrayDefaults(): void
    {
        $response = ConsumerPauseResponse::fromArray([]);

        self::assertFalse($response->paused);
        self::assertNull($response->pauseUntil);
        self::assertNull($response->pauseRemaining);
    }

    public function testNsConversionPrecision(): void
    {
        $response = ConsumerPauseResponse::fromArray([
            'paused' => true,
            'pause_remaining' => 1_500_000_000,
        ]);

        self::assertSame(1.5, $response->pauseRemaining);
    }
}
