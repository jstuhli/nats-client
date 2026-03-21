<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\ClusterInfo;
use Nats\JetStream\Stream\PeerInfo;
use Nats\JetStream\Stream\StreamAlternate;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamInfo;
use Nats\JetStream\Stream\StreamSourceInfo;
use Nats\JetStream\Stream\StreamState;
use PHPUnit\Framework\TestCase;

final class StreamInfoTest extends TestCase
{
    public function testFromArrayMinimal(): void
    {
        $data = [
            'config' => ['name' => 'test-stream'],
            'state' => [
                'messages' => 100,
                'bytes' => 2048,
                'first_seq' => 1,
                'last_seq' => 100,
            ],
        ];

        $info = StreamInfo::fromArray($data);

        self::assertInstanceOf(StreamConfig::class, $info->config);
        self::assertSame('test-stream', $info->config->name);
        self::assertInstanceOf(StreamState::class, $info->state);
        self::assertSame(100, $info->state->messages);
        self::assertNull($info->created);
        self::assertNull($info->cluster);
        self::assertNull($info->mirror);
        self::assertEmpty($info->sources);
        self::assertEmpty($info->alternates);
    }

    public function testFromArrayWithCluster(): void
    {
        $data = [
            'config' => ['name' => 'clustered'],
            'state' => ['messages' => 0, 'bytes' => 0, 'first_seq' => 0, 'last_seq' => 0],
            'cluster' => [
                'name' => 'my-cluster',
                'leader' => 'node-a',
                'replicas' => [
                    ['name' => 'node-b', 'current' => true, 'lag' => 0, 'active' => 500_000_000],
                ],
            ],
        ];

        $info = StreamInfo::fromArray($data);

        self::assertInstanceOf(ClusterInfo::class, $info->cluster);
        self::assertSame('my-cluster', $info->cluster->name);
        self::assertSame('node-a', $info->cluster->leader);
        self::assertCount(1, $info->cluster->replicas);
        self::assertInstanceOf(PeerInfo::class, $info->cluster->replicas[0]);
        self::assertSame('node-b', $info->cluster->replicas[0]->name);
    }

    public function testFromArrayWithMirror(): void
    {
        $data = [
            'config' => ['name' => 'mirror-stream'],
            'state' => ['messages' => 50, 'bytes' => 1024, 'first_seq' => 1, 'last_seq' => 50],
            'mirror' => [
                'name' => 'origin-stream',
                'lag' => 3,
                'active' => 1_000_000_000,
            ],
        ];

        $info = StreamInfo::fromArray($data);

        self::assertInstanceOf(StreamSourceInfo::class, $info->mirror);
        self::assertSame('origin-stream', $info->mirror->name);
        self::assertSame(3, $info->mirror->lag);
        self::assertSame(1.0, $info->mirror->active);
    }

    public function testFromArrayWithSources(): void
    {
        $data = [
            'config' => ['name' => 'aggregate'],
            'state' => ['messages' => 0, 'bytes' => 0, 'first_seq' => 0, 'last_seq' => 0],
            'sources' => [
                ['name' => 'source-a', 'lag' => 0, 'active' => 100_000_000],
                ['name' => 'source-b', 'lag' => 10, 'active' => 2_000_000_000],
            ],
        ];

        $info = StreamInfo::fromArray($data);

        self::assertCount(2, $info->sources);
        self::assertInstanceOf(StreamSourceInfo::class, $info->sources[0]);
        self::assertSame('source-a', $info->sources[0]->name);
        self::assertSame('source-b', $info->sources[1]->name);
        self::assertSame(10, $info->sources[1]->lag);
    }

    public function testFromArrayWithAlternates(): void
    {
        $data = [
            'config' => ['name' => 'replicated'],
            'state' => ['messages' => 0, 'bytes' => 0, 'first_seq' => 0, 'last_seq' => 0],
            'alternates' => [
                ['name' => 'replicated', 'domain' => 'hub', 'cluster' => 'east'],
                ['name' => 'replicated', 'domain' => 'hub', 'cluster' => 'west'],
            ],
        ];

        $info = StreamInfo::fromArray($data);

        self::assertCount(2, $info->alternates);
        self::assertInstanceOf(StreamAlternate::class, $info->alternates[0]);
        self::assertSame('east', $info->alternates[0]->cluster);
        self::assertSame('west', $info->alternates[1]->cluster);
    }

    public function testFromArrayWithCreated(): void
    {
        $data = [
            'config' => ['name' => 'dated'],
            'state' => ['messages' => 0, 'bytes' => 0, 'first_seq' => 0, 'last_seq' => 0],
            'created' => '2026-01-15T10:30:00+00:00',
        ];

        $info = StreamInfo::fromArray($data);

        self::assertInstanceOf(\DateTimeImmutable::class, $info->created);
        self::assertSame('2026-01-15', $info->created->format('Y-m-d'));
    }
}
