<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\ObjectStore;

use Nats\ObjectStore\ObjectLink;
use PHPUnit\Framework\TestCase;

final class ObjectLinkTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $link = new ObjectLink();

        self::assertSame('', $link->bucket);
        self::assertSame('', $link->name);
    }

    public function testConstructorWithValues(): void
    {
        $link = new ObjectLink(bucket: 'my-bucket', name: 'my-object');

        self::assertSame('my-bucket', $link->bucket);
        self::assertSame('my-object', $link->name);
    }

    public function testToArray(): void
    {
        $link = new ObjectLink(bucket: 'photos', name: 'photo.jpg');
        $array = $link->toArray();

        self::assertSame('photos', $array['bucket']);
        self::assertSame('photo.jpg', $array['name']);
    }

    public function testToArrayOmitsEmptyStrings(): void
    {
        $link = new ObjectLink();
        $array = $link->toArray();

        self::assertEmpty($array);
    }

    public function testToArrayPartialBucket(): void
    {
        $link = new ObjectLink(bucket: 'store');
        $array = $link->toArray();

        self::assertSame('store', $array['bucket']);
        self::assertArrayNotHasKey('name', $array);
    }

    public function testToArrayPartialName(): void
    {
        $link = new ObjectLink(name: 'file.txt');
        $array = $link->toArray();

        self::assertArrayNotHasKey('bucket', $array);
        self::assertSame('file.txt', $array['name']);
    }

    public function testFromArray(): void
    {
        $link = ObjectLink::fromArray(['bucket' => 'docs', 'name' => 'readme.md']);

        self::assertSame('docs', $link->bucket);
        self::assertSame('readme.md', $link->name);
    }

    public function testFromArrayDefaults(): void
    {
        $link = ObjectLink::fromArray([]);

        self::assertSame('', $link->bucket);
        self::assertSame('', $link->name);
    }

    public function testRoundTrip(): void
    {
        $original = new ObjectLink(bucket: 'assets', name: 'logo.png');
        $restored = ObjectLink::fromArray($original->toArray());

        self::assertSame($original->bucket, $restored->bucket);
        self::assertSame($original->name, $restored->name);
    }
}
