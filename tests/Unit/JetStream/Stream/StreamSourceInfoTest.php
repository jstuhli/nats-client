<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\StreamSourceInfo;
use Nats\JetStream\Stream\SubjectTransform;
use PHPUnit\Framework\TestCase;

final class StreamSourceInfoTest extends TestCase
{
    public function testFromArrayFull(): void
    {
        $data = [
            'name' => 'origin-stream',
            'lag' => 42,
            'active' => 2_500_000_000,
            'filter_subject' => 'events.>',
            'subject_transforms' => [
                ['src' => 'events.>', 'dest' => 'mirrored.events.>'],
                ['src' => 'orders.>', 'dest' => 'mirrored.orders.>'],
            ],
        ];

        $info = StreamSourceInfo::fromArray($data);

        self::assertSame('origin-stream', $info->name);
        self::assertSame(42, $info->lag);
        self::assertSame(2.5, $info->active);
        self::assertSame('events.>', $info->filterSubject);
        self::assertCount(2, $info->subjectTransforms);

        self::assertInstanceOf(SubjectTransform::class, $info->subjectTransforms[0]);
        self::assertSame('events.>', $info->subjectTransforms[0]->source);
        self::assertSame('mirrored.events.>', $info->subjectTransforms[0]->destination);

        self::assertSame('orders.>', $info->subjectTransforms[1]->source);
        self::assertSame('mirrored.orders.>', $info->subjectTransforms[1]->destination);
    }

    public function testFromArrayDefaults(): void
    {
        $info = StreamSourceInfo::fromArray(['name' => 'test']);

        self::assertSame('test', $info->name);
        self::assertSame(0, $info->lag);
        self::assertSame(0.0, $info->active);
        self::assertNull($info->filterSubject);
        self::assertEmpty($info->subjectTransforms);
    }

    public function testFromArrayEmpty(): void
    {
        $info = StreamSourceInfo::fromArray([]);

        self::assertSame('', $info->name);
        self::assertSame(0, $info->lag);
        self::assertSame(0.0, $info->active);
    }
}
