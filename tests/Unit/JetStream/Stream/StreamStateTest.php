<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\StreamState;
use PHPUnit\Framework\TestCase;

final class StreamStateTest extends TestCase
{
    public function testFromArrayWithSubjects(): void
    {
        $data = [
            'messages' => 10,
            'bytes' => 2048,
            'first_seq' => 1,
            'first_ts' => '2025-01-01T00:00:00Z',
            'last_seq' => 10,
            'last_ts' => '2025-01-01T01:00:00Z',
            'num_subjects' => 3,
            'subjects' => [
                'orders.created' => 4,
                'orders.updated' => 3,
                'orders.deleted' => 3,
            ],
        ];

        $state = StreamState::fromArray($data);

        self::assertSame(10, $state->messages);
        self::assertSame(2048, $state->bytes);
        self::assertSame(3, $state->numSubjects);
        self::assertCount(3, $state->subjects);
        self::assertSame(4, $state->subjects['orders.created']);
        self::assertSame(3, $state->subjects['orders.updated']);
        self::assertSame(3, $state->subjects['orders.deleted']);
    }

    public function testFromArrayWithDeleted(): void
    {
        $data = [
            'messages' => 7,
            'bytes' => 1024,
            'first_seq' => 1,
            'first_ts' => '2025-01-01T00:00:00Z',
            'last_seq' => 10,
            'last_ts' => '2025-01-01T01:00:00Z',
            'num_deleted' => 3,
            'deleted' => [2, 5, 8],
        ];

        $state = StreamState::fromArray($data);

        self::assertSame(7, $state->messages);
        self::assertSame(3, $state->numDeleted);
        self::assertCount(3, $state->deleted);
        self::assertSame([2, 5, 8], $state->deleted);
    }

    public function testFromArrayMinimal(): void
    {
        $state = StreamState::fromArray([]);

        self::assertSame(0, $state->messages);
        self::assertSame(0, $state->bytes);
        self::assertSame(0, $state->firstSeq);
        self::assertNull($state->firstTs);
        self::assertSame(0, $state->lastSeq);
        self::assertNull($state->lastTs);
        self::assertSame(0, $state->numSubjects);
        self::assertSame(0, $state->numDeleted);
        self::assertSame(0, $state->consumers);
        self::assertSame([], $state->subjects);
        self::assertSame([], $state->deleted);
    }

    public function testFromArrayWithConsumerCount(): void
    {
        $data = [
            'messages' => 5,
            'bytes' => 512,
            'first_seq' => 1,
            'last_seq' => 5,
            'consumer_count' => 3,
        ];

        $state = StreamState::fromArray($data);

        self::assertSame(3, $state->consumers);
    }

    public function testFromArrayWithSubjectsAndDeleted(): void
    {
        $data = [
            'messages' => 8,
            'bytes' => 4096,
            'first_seq' => 1,
            'first_ts' => '2025-06-01T00:00:00Z',
            'last_seq' => 12,
            'last_ts' => '2025-06-01T12:00:00Z',
            'num_subjects' => 2,
            'num_deleted' => 4,
            'consumer_count' => 1,
            'subjects' => [
                'events.click' => 5,
                'events.view' => 3,
            ],
            'deleted' => [3, 6, 9, 11],
        ];

        $state = StreamState::fromArray($data);

        self::assertSame(8, $state->messages);
        self::assertSame(4096, $state->bytes);
        self::assertSame(2, $state->numSubjects);
        self::assertSame(4, $state->numDeleted);
        self::assertSame(1, $state->consumers);
        self::assertSame(5, $state->subjects['events.click']);
        self::assertSame(3, $state->subjects['events.view']);
        self::assertSame([3, 6, 9, 11], $state->deleted);
    }
}
