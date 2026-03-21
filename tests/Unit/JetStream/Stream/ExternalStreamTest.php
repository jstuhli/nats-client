<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\ExternalStream;
use PHPUnit\Framework\TestCase;

final class ExternalStreamTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $ext = new ExternalStream();

        self::assertSame('', $ext->apiPrefix);
        self::assertSame('', $ext->deliverPrefix);
    }

    public function testConstructorWithValues(): void
    {
        $ext = new ExternalStream(apiPrefix: '$JS.domain.API', deliverPrefix: '$JS.domain.ACK');

        self::assertSame('$JS.domain.API', $ext->apiPrefix);
        self::assertSame('$JS.domain.ACK', $ext->deliverPrefix);
    }

    public function testToArray(): void
    {
        $ext = new ExternalStream(apiPrefix: '$JS.hub.API', deliverPrefix: '$JS.hub.ACK');
        $array = $ext->toArray();

        self::assertSame('$JS.hub.API', $array['api']);
        self::assertSame('$JS.hub.ACK', $array['deliver']);
    }

    public function testToArrayOmitsEmptyStrings(): void
    {
        $ext = new ExternalStream();
        $array = $ext->toArray();

        self::assertEmpty($array);
    }

    public function testToArrayPartial(): void
    {
        $ext = new ExternalStream(apiPrefix: '$JS.test.API');
        $array = $ext->toArray();

        self::assertSame('$JS.test.API', $array['api']);
        self::assertArrayNotHasKey('deliver', $array);
    }

    public function testFromArray(): void
    {
        $ext = ExternalStream::fromArray(['api' => '$JS.remote.API', 'deliver' => '$JS.remote.ACK']);

        self::assertSame('$JS.remote.API', $ext->apiPrefix);
        self::assertSame('$JS.remote.ACK', $ext->deliverPrefix);
    }

    public function testFromArrayDefaults(): void
    {
        $ext = ExternalStream::fromArray([]);

        self::assertSame('', $ext->apiPrefix);
        self::assertSame('', $ext->deliverPrefix);
    }

    public function testRoundTrip(): void
    {
        $original = new ExternalStream(apiPrefix: '$JS.east.API', deliverPrefix: '$JS.east.ACK');
        $restored = ExternalStream::fromArray($original->toArray());

        self::assertSame($original->apiPrefix, $restored->apiPrefix);
        self::assertSame($original->deliverPrefix, $restored->deliverPrefix);
    }
}
