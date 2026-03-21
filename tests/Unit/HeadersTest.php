<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Headers;
use PHPUnit\Framework\TestCase;

final class HeadersTest extends TestCase
{
    public function testSetAndGet(): void
    {
        $headers = new Headers();
        $headers->set('X-Test', 'value1');

        self::assertSame('value1', $headers->get('X-Test'));
    }

    public function testGetNonExistent(): void
    {
        $headers = new Headers();
        self::assertNull($headers->get('Missing'));
    }

    public function testAddMultipleValues(): void
    {
        $headers = new Headers();
        $headers->add('X-Multi', 'a');
        $headers->add('X-Multi', 'b');

        self::assertSame('a', $headers->get('X-Multi'));
        self::assertSame(['a', 'b'], $headers->values('X-Multi'));
    }

    public function testHas(): void
    {
        $headers = new Headers();
        self::assertFalse($headers->has('X-Test'));

        $headers->set('X-Test', 'val');
        self::assertTrue($headers->has('X-Test'));
    }

    public function testDelete(): void
    {
        $headers = new Headers();
        $headers->set('X-Test', 'val');
        $headers->delete('X-Test');

        self::assertFalse($headers->has('X-Test'));
    }

    public function testCount(): void
    {
        $headers = new Headers();
        self::assertSame(0, $headers->count());

        $headers->set('A', '1');
        $headers->set('B', '2');
        self::assertSame(2, $headers->count());
    }

    public function testConstructorWithArray(): void
    {
        $headers = new Headers(['X-Foo' => 'bar', 'X-Multi' => ['a', 'b']]);

        self::assertSame('bar', $headers->get('X-Foo'));
        self::assertSame(['a', 'b'], $headers->values('X-Multi'));
    }

    public function testToWireFormat(): void
    {
        $headers = new Headers();
        $headers->set('X-Test', 'value');
        $headers->add('X-Multi', 'a');
        $headers->add('X-Multi', 'b');

        $wire = $headers->toWireFormat();
        self::assertStringStartsWith("NATS/1.0\r\n", $wire);
        self::assertStringContainsString("X-Test: value\r\n", $wire);
        self::assertStringContainsString("X-Multi: a\r\n", $wire);
        self::assertStringContainsString("X-Multi: b\r\n", $wire);
        self::assertStringEndsWith("\r\n\r\n", $wire);
    }

    public function testFromWireFormat(): void
    {
        $wire = "NATS/1.0\r\nX-Test: value\r\nX-Multi: a\r\nX-Multi: b\r\n\r\n";
        $headers = Headers::fromWireFormat($wire);

        self::assertSame('value', $headers->get('X-Test'));
        self::assertSame(['a', 'b'], $headers->values('X-Multi'));
    }

    public function testFromWireFormatWithStatus(): void
    {
        $wire = "NATS/1.0 404 No Messages\r\n\r\n";
        $headers = Headers::fromWireFormat($wire);

        self::assertSame('404 No Messages', $headers->get('Status'));
    }

    public function testAll(): void
    {
        $headers = new Headers(['A' => '1', 'B' => ['x', 'y']]);
        $all = $headers->all();

        self::assertSame(['1'], $all['A']);
        self::assertSame(['x', 'y'], $all['B']);
    }

    public function testIterable(): void
    {
        $headers = new Headers(['A' => '1', 'B' => '2']);
        $keys = [];
        foreach ($headers as $key => $values) {
            $keys[] = $key;
        }
        self::assertSame(['A', 'B'], $keys);
    }
}
