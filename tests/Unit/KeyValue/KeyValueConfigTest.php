<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\KeyValue;

use Nats\JetStream\Stream\ExternalStream;
use Nats\JetStream\Stream\RePublish;
use Nats\JetStream\Stream\StreamSource;
use Nats\JetStream\Stream\SubjectTransform;
use Nats\KeyValue\KeyValueConfig;
use PHPUnit\Framework\TestCase;

final class KeyValueConfigTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $config = new KeyValueConfig(bucket: 'test-kv');

        self::assertSame('test-kv', $config->bucket);
        self::assertNull($config->description);
        self::assertNull($config->maxBytes);
        self::assertSame(1, $config->history);
        self::assertNull($config->ttl);
        self::assertNull($config->maxValueSize);
        self::assertSame(1, $config->replicas);
        self::assertNull($config->placement);
        self::assertSame([], $config->metadata);
        self::assertNull($config->mirror);
        self::assertSame([], $config->sources);
        self::assertFalse($config->compression);
        self::assertNull($config->rePublish);
        self::assertNull($config->limitMarkerTtl);
    }

    public function testConstructorWithMirror(): void
    {
        $mirror = new StreamSource(name: 'origin-kv');
        $config = new KeyValueConfig(bucket: 'mirror-kv', mirror: $mirror);

        self::assertSame($mirror, $config->mirror);
        self::assertSame('origin-kv', $config->mirror->name);
    }

    public function testConstructorWithSources(): void
    {
        $sources = [
            new StreamSource(name: 'kv-a'),
            new StreamSource(name: 'kv-b'),
        ];
        $config = new KeyValueConfig(bucket: 'agg-kv', sources: $sources);

        self::assertCount(2, $config->sources);
        self::assertSame('kv-a', $config->sources[0]->name);
        self::assertSame('kv-b', $config->sources[1]->name);
    }

    public function testConstructorWithCompression(): void
    {
        $config = new KeyValueConfig(bucket: 'compressed', compression: true);

        self::assertTrue($config->compression);
    }

    public function testConstructorWithRePublish(): void
    {
        $rp = new RePublish(source: '>', destination: 'republished.>');
        $config = new KeyValueConfig(bucket: 'rp-kv', rePublish: $rp);

        self::assertSame($rp, $config->rePublish);
        self::assertSame('>', $config->rePublish->source);
        self::assertSame('republished.>', $config->rePublish->destination);
    }

    public function testConstructorWithLimitMarkerTtl(): void
    {
        $config = new KeyValueConfig(bucket: 'ttl-kv', limitMarkerTtl: 300.0);

        self::assertSame(300.0, $config->limitMarkerTtl);
    }

    public function testConstructorWithAllNewFields(): void
    {
        $config = new KeyValueConfig(
            bucket: 'full-kv',
            description: 'Full featured KV',
            history: 5,
            replicas: 3,
            mirror: new StreamSource(name: 'source-kv'),
            sources: [new StreamSource(name: 'other-kv')],
            compression: true,
            rePublish: new RePublish(source: '>', destination: 're.>'),
            limitMarkerTtl: 60.0,
        );

        self::assertSame('full-kv', $config->bucket);
        self::assertSame('Full featured KV', $config->description);
        self::assertSame(5, $config->history);
        self::assertSame(3, $config->replicas);
        self::assertNotNull($config->mirror);
        self::assertSame('source-kv', $config->mirror->name);
        self::assertCount(1, $config->sources);
        self::assertTrue($config->compression);
        self::assertNotNull($config->rePublish);
        self::assertSame(60.0, $config->limitMarkerTtl);
    }
}
