<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\Enum\DiscardPolicy;
use Nats\Enum\RetentionPolicy;
use Nats\Enum\StorageType;
use Nats\Enum\StoreCompression;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamConsumerLimits;
use Nats\JetStream\Stream\StreamSource;
use Nats\JetStream\Stream\SubjectTransform;
use PHPUnit\Framework\TestCase;

final class StreamConfigTest extends TestCase
{
    public function testConstructorDefaults(): void
    {
        $config = new StreamConfig(name: 'test');

        self::assertSame('test', $config->name);
        self::assertSame([], $config->subjects);
        self::assertNull($config->description);
        self::assertSame(RetentionPolicy::Limits, $config->retention);
        self::assertNull($config->maxConsumers);
        self::assertNull($config->subjectTransform);
        self::assertFalse($config->mirrorDirect);
        self::assertNull($config->consumerLimits);
        self::assertFalse($config->allowMsgTtl);
        self::assertNull($config->subjectDeleteMarkerTtl);
    }

    public function testConstructorWithAllNewFields(): void
    {
        $transform = new SubjectTransform(source: 'foo.>', destination: 'bar.>');
        $limits = new StreamConsumerLimits(inactiveThreshold: 30.0, maxAckPending: 500);

        $config = new StreamConfig(
            name: 'full-test',
            subjects: ['orders.>'],
            maxConsumers: 10,
            mirrorDirect: true,
            subjectTransform: $transform,
            consumerLimits: $limits,
            allowMsgTtl: true,
            subjectDeleteMarkerTtl: 3600,
        );

        self::assertSame('full-test', $config->name);
        self::assertSame(['orders.>'], $config->subjects);
        self::assertSame(10, $config->maxConsumers);
        self::assertTrue($config->mirrorDirect);
        self::assertSame($transform, $config->subjectTransform);
        self::assertSame($limits, $config->consumerLimits);
        self::assertTrue($config->allowMsgTtl);
        self::assertSame(3600, $config->subjectDeleteMarkerTtl);
    }

    public function testToArraySerializesNewFields(): void
    {
        $config = new StreamConfig(
            name: 'serialize-test',
            subjects: ['events.>'],
            maxConsumers: 5,
            mirrorDirect: true,
            subjectTransform: new SubjectTransform(source: 'a.>', destination: 'b.>'),
            consumerLimits: new StreamConsumerLimits(inactiveThreshold: 10.0, maxAckPending: 200),
            allowMsgTtl: true,
            subjectDeleteMarkerTtl: 120,
        );

        $array = $config->toArray();

        self::assertSame('serialize-test', $array['name']);
        self::assertSame(['events.>'], $array['subjects']);
        self::assertSame(5, $array['max_consumers']);
        self::assertTrue($array['mirror_direct']);
        self::assertSame(['src' => 'a.>', 'dest' => 'b.>'], $array['subject_transform']);
        self::assertSame(10_000_000_000, $array['consumer_limits']['inactive_threshold']);
        self::assertSame(200, $array['consumer_limits']['max_ack_pending']);
        self::assertTrue($array['allow_msg_ttl']);
        self::assertSame(120_000_000_000, $array['subject_delete_marker_ttl']);
    }

    public function testToArrayOmitsNullNewFields(): void
    {
        $config = new StreamConfig(name: 'minimal');
        $array = $config->toArray();

        self::assertArrayNotHasKey('max_consumers', $array);
        self::assertArrayNotHasKey('subject_transform', $array);
        self::assertArrayNotHasKey('consumer_limits', $array);
        self::assertArrayNotHasKey('allow_msg_ttl', $array);
        self::assertArrayNotHasKey('subject_delete_marker_ttl', $array);
        self::assertArrayNotHasKey('mirror_direct', $array);
    }

    public function testFromArray(): void
    {
        $data = [
            'name' => 'from-array',
            'subjects' => ['test.>'],
            'retention' => 'limits',
            'storage' => 'file',
            'discard' => 'old',
            'num_replicas' => 3,
            'max_consumers' => 8,
            'mirror_direct' => true,
            'subject_transform' => ['src' => 'in.>', 'dest' => 'out.>'],
            'consumer_limits' => [
                'inactive_threshold' => 20_000_000_000,
                'max_ack_pending' => 100,
            ],
            'allow_msg_ttl' => true,
            'subject_delete_marker_ttl' => 60_000_000_000,
        ];

        $config = StreamConfig::fromArray($data);

        self::assertSame('from-array', $config->name);
        self::assertSame(8, $config->maxConsumers);
        self::assertTrue($config->mirrorDirect);
        self::assertInstanceOf(SubjectTransform::class, $config->subjectTransform);
        self::assertSame('in.>', $config->subjectTransform->source);
        self::assertSame('out.>', $config->subjectTransform->destination);
        self::assertInstanceOf(StreamConsumerLimits::class, $config->consumerLimits);
        self::assertSame(20.0, $config->consumerLimits->inactiveThreshold);
        self::assertSame(100, $config->consumerLimits->maxAckPending);
        self::assertTrue($config->allowMsgTtl);
        self::assertSame(60, $config->subjectDeleteMarkerTtl);
    }

    public function testRoundTrip(): void
    {
        $original = new StreamConfig(
            name: 'roundtrip',
            subjects: ['orders.*', 'events.*'],
            description: 'Test stream',
            maxConsumers: 3,
            mirrorDirect: true,
            subjectTransform: new SubjectTransform(source: 'x.>', destination: 'y.>'),
            consumerLimits: new StreamConsumerLimits(inactiveThreshold: 45.0, maxAckPending: 750),
            allowMsgTtl: true,
            subjectDeleteMarkerTtl: 300,
        );

        $restored = StreamConfig::fromArray($original->toArray());

        self::assertSame($original->name, $restored->name);
        self::assertSame($original->subjects, $restored->subjects);
        self::assertSame($original->description, $restored->description);
        self::assertSame($original->maxConsumers, $restored->maxConsumers);
        self::assertSame($original->mirrorDirect, $restored->mirrorDirect);
        self::assertSame($original->subjectTransform->source, $restored->subjectTransform->source);
        self::assertSame($original->subjectTransform->destination, $restored->subjectTransform->destination);
        self::assertSame($original->consumerLimits->inactiveThreshold, $restored->consumerLimits->inactiveThreshold);
        self::assertSame($original->consumerLimits->maxAckPending, $restored->consumerLimits->maxAckPending);
        self::assertSame($original->allowMsgTtl, $restored->allowMsgTtl);
        self::assertSame($original->subjectDeleteMarkerTtl, $restored->subjectDeleteMarkerTtl);
    }

    public function testMaxAgeNsConversion(): void
    {
        $config = new StreamConfig(name: 'age-test', maxAge: 3600);
        $array = $config->toArray();

        self::assertSame(3_600_000_000_000, $array['max_age']);

        $restored = StreamConfig::fromArray($array);
        self::assertSame(3600, $restored->maxAge);
    }

    public function testDuplicateWindowNsConversion(): void
    {
        $config = new StreamConfig(name: 'dup-test', duplicateWindow: 120);
        $array = $config->toArray();

        self::assertSame(120_000_000_000, $array['duplicate_window']);

        $restored = StreamConfig::fromArray($array);
        self::assertSame(120, $restored->duplicateWindow);
    }
}
