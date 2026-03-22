<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\ObjectStore;

use Nats\ObjectStore\ObjectLink;
use Nats\ObjectStore\ObjectMeta;
use PHPUnit\Framework\TestCase;

final class ObjectMetaTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $meta = new ObjectMeta(name: 'test-object');

        self::assertSame('test-object', $meta->name);
        self::assertNull($meta->description);
        self::assertNull($meta->headers);
        self::assertNull($meta->link);
        self::assertSame([], $meta->metadata);
        self::assertNull($meta->chunkSize);
    }

    public function testConstructorWithNewFields(): void
    {
        $link = new ObjectLink(bucket: 'other', name: 'target');
        $meta = new ObjectMeta(
            name: 'linked-object',
            description: 'A linked object',
            link: $link,
            metadata: ['key' => 'value'],
            chunkSize: 65536,
        );

        self::assertSame('linked-object', $meta->name);
        self::assertSame('A linked object', $meta->description);
        self::assertSame($link, $meta->link);
        self::assertSame(['key' => 'value'], $meta->metadata);
        self::assertSame(65536, $meta->chunkSize);
    }

    public function testToArrayMinimal(): void
    {
        $meta = new ObjectMeta(name: 'simple');
        $array = $meta->toArray();

        self::assertSame(['name' => 'simple'], $array);
    }

    public function testToArrayWithLink(): void
    {
        $meta = new ObjectMeta(
            name: 'with-link',
            link: new ObjectLink(bucket: 'source', name: 'original'),
        );
        $array = $meta->toArray();

        self::assertSame('source', $array['link']['bucket']);
        self::assertSame('original', $array['link']['name']);
    }

    public function testToArrayWithMetadata(): void
    {
        $meta = new ObjectMeta(
            name: 'with-meta',
            metadata: ['env' => 'prod', 'version' => '2'],
        );
        $array = $meta->toArray();

        self::assertSame(['env' => 'prod', 'version' => '2'], $array['metadata']);
    }

    public function testToArrayWithChunkSize(): void
    {
        $meta = new ObjectMeta(name: 'chunked', chunkSize: 131072);
        $array = $meta->toArray();

        self::assertSame(131072, $array['options']['max_chunk_size']);
    }

    public function testToArrayOmitsNulls(): void
    {
        $meta = new ObjectMeta(name: 'clean');
        $array = $meta->toArray();

        self::assertArrayNotHasKey('description', $array);
        self::assertArrayNotHasKey('link', $array);
        self::assertArrayNotHasKey('metadata', $array);
        self::assertArrayNotHasKey('options', $array);
    }

    public function testFromArray(): void
    {
        $data = [
            'name' => 'restored',
            'description' => 'Restored object',
            'link' => ['bucket' => 'archive', 'name' => 'backup'],
            'metadata' => ['tag' => 'important'],
            'options' => ['max_chunk_size' => 32768],
        ];

        $meta = ObjectMeta::fromArray($data);

        self::assertSame('restored', $meta->name);
        self::assertSame('Restored object', $meta->description);
        self::assertInstanceOf(ObjectLink::class, $meta->link);
        self::assertSame('archive', $meta->link->bucket);
        self::assertSame('backup', $meta->link->name);
        self::assertSame(['tag' => 'important'], $meta->metadata);
        self::assertSame(32768, $meta->chunkSize);
    }

    public function testFromArrayDefaults(): void
    {
        $meta = ObjectMeta::fromArray(['name' => 'bare']);

        self::assertSame('bare', $meta->name);
        self::assertNull($meta->description);
        self::assertNull($meta->link);
        self::assertSame([], $meta->metadata);
        self::assertNull($meta->chunkSize);
    }

    public function testConstructorRejectsNonPositiveChunkSize(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('chunkSize must be greater than 0');

        new ObjectMeta(name: 'invalid', chunkSize: 0);
    }
}
