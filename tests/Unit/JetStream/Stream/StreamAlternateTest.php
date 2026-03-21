<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\StreamAlternate;
use PHPUnit\Framework\TestCase;

final class StreamAlternateTest extends TestCase
{
    public function testFromArray(): void
    {
        $alt = StreamAlternate::fromArray([
            'name' => 'my-stream',
            'domain' => 'hub',
            'cluster' => 'east',
        ]);

        self::assertSame('my-stream', $alt->name);
        self::assertSame('hub', $alt->domain);
        self::assertSame('east', $alt->cluster);
    }

    public function testFromArrayDefaults(): void
    {
        $alt = StreamAlternate::fromArray([]);

        self::assertSame('', $alt->name);
        self::assertSame('', $alt->domain);
        self::assertSame('', $alt->cluster);
    }

    public function testFromArrayPartial(): void
    {
        $alt = StreamAlternate::fromArray(['name' => 'orders']);

        self::assertSame('orders', $alt->name);
        self::assertSame('', $alt->domain);
        self::assertSame('', $alt->cluster);
    }
}
