<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\SubjectTransform;
use PHPUnit\Framework\TestCase;

final class SubjectTransformTest extends TestCase
{
    public function testConstructor(): void
    {
        $transform = new SubjectTransform(source: 'foo.>', destination: 'bar.>');

        self::assertSame('foo.>', $transform->source);
        self::assertSame('bar.>', $transform->destination);
    }

    public function testToArray(): void
    {
        $transform = new SubjectTransform(source: 'orders.*', destination: 'archive.orders.*');
        $array = $transform->toArray();

        self::assertSame('orders.*', $array['src']);
        self::assertSame('archive.orders.*', $array['dest']);
        self::assertCount(2, $array);
    }

    public function testFromArray(): void
    {
        $data = ['src' => 'input.>', 'dest' => 'output.>'];
        $transform = SubjectTransform::fromArray($data);

        self::assertSame('input.>', $transform->source);
        self::assertSame('output.>', $transform->destination);
    }

    public function testFromArrayDefaults(): void
    {
        $transform = SubjectTransform::fromArray([]);

        self::assertSame('', $transform->source);
        self::assertSame('', $transform->destination);
    }

    public function testRoundTrip(): void
    {
        $original = new SubjectTransform(source: 'events.>', destination: 'processed.events.>');
        $restored = SubjectTransform::fromArray($original->toArray());

        self::assertSame($original->source, $restored->source);
        self::assertSame($original->destination, $restored->destination);
    }
}
