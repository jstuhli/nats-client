<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\ObjectStore;

use Nats\ObjectStore\ObjectInfo;
use Nats\ObjectStore\ObjectLink;
use PHPUnit\Framework\TestCase;

final class ObjectInfoTest extends TestCase
{
    public function testFromArrayMinimal(): void
    {
        $data = [
            'name' => 'test.bin',
            'bucket' => 'files',
            'nuid' => 'abc123',
            'size' => 1024,
            'chunks' => 1,
            'digest' => 'SHA-256=xyz',
            'deleted' => false,
        ];

        $info = ObjectInfo::fromArray($data);

        self::assertSame('test.bin', $info->name);
        self::assertSame('files', $info->bucket);
        self::assertSame('abc123', $info->nuid);
        self::assertSame(1024, $info->size);
        self::assertSame(1, $info->chunks);
        self::assertSame('SHA-256=xyz', $info->digest);
        self::assertFalse($info->deleted);
        self::assertNull($info->link);
        self::assertSame([], $info->metadata);
    }

    public function testFromArrayWithLink(): void
    {
        $data = [
            'name' => 'linked.txt',
            'bucket' => 'docs',
            'nuid' => 'nuid1',
            'size' => 0,
            'chunks' => 0,
            'digest' => '',
            'deleted' => false,
            'link' => ['bucket' => 'source-bucket', 'name' => 'original.txt'],
        ];

        $info = ObjectInfo::fromArray($data);

        self::assertInstanceOf(ObjectLink::class, $info->link);
        self::assertSame('source-bucket', $info->link->bucket);
        self::assertSame('original.txt', $info->link->name);
    }

    public function testFromArrayWithMetadata(): void
    {
        $data = [
            'name' => 'tagged.dat',
            'bucket' => 'store',
            'nuid' => 'nuid2',
            'size' => 512,
            'chunks' => 1,
            'digest' => 'SHA-256=abc',
            'deleted' => false,
            'metadata' => ['type' => 'report', 'year' => '2026'],
        ];

        $info = ObjectInfo::fromArray($data);

        self::assertSame(['type' => 'report', 'year' => '2026'], $info->metadata);
    }

    public function testIsLinkReturnsTrueWhenLinkHasBucket(): void
    {
        $info = new ObjectInfo(
            name: 'link-obj',
            bucket: 'my-bucket',
            nuid: 'n1',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            link: new ObjectLink(bucket: 'other-bucket'),
        );

        self::assertTrue($info->isLink());
    }

    public function testIsLinkReturnsTrueWhenLinkHasName(): void
    {
        $info = new ObjectInfo(
            name: 'link-obj',
            bucket: 'my-bucket',
            nuid: 'n1',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            link: new ObjectLink(name: 'target-name'),
        );

        self::assertTrue($info->isLink());
    }

    public function testIsLinkReturnsFalseWhenNoLink(): void
    {
        $info = new ObjectInfo(
            name: 'regular',
            bucket: 'my-bucket',
            nuid: 'n1',
            size: 100,
            chunks: 1,
            digest: 'SHA-256=abc',
            deleted: false,
        );

        self::assertFalse($info->isLink());
    }

    public function testIsLinkReturnsFalseWhenLinkEmpty(): void
    {
        $info = new ObjectInfo(
            name: 'empty-link',
            bucket: 'my-bucket',
            nuid: 'n1',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            link: new ObjectLink(),
        );

        self::assertFalse($info->isLink());
    }

    public function testFromArrayWithMtime(): void
    {
        $data = [
            'name' => 'timed.dat',
            'bucket' => 'store',
            'nuid' => 'n3',
            'size' => 256,
            'chunks' => 1,
            'digest' => 'SHA-256=def',
            'deleted' => false,
            'mtime' => '2026-03-15T08:00:00+00:00',
        ];

        $info = ObjectInfo::fromArray($data);

        self::assertInstanceOf(\DateTimeImmutable::class, $info->mtime);
        self::assertSame('2026-03-15', $info->mtime->format('Y-m-d'));
    }

    public function testFromArrayDefaults(): void
    {
        $info = ObjectInfo::fromArray([]);

        self::assertSame('', $info->name);
        self::assertSame('', $info->bucket);
        self::assertSame(0, $info->size);
        self::assertFalse($info->deleted);
        self::assertNull($info->link);
        self::assertSame([], $info->metadata);
    }
}
