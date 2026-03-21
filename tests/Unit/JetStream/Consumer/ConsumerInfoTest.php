<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Consumer;

use Nats\JetStream\Consumer\ConsumerInfo;
use Nats\JetStream\Stream\ClusterInfo;
use PHPUnit\Framework\TestCase;

final class ConsumerInfoTest extends TestCase
{
    public function testFromArrayWithCluster(): void
    {
        $data = [
            'stream_name' => 'TEST_STREAM',
            'name' => 'test-consumer',
            'config' => [
                'ack_policy' => 'explicit',
                'deliver_policy' => 'all',
            ],
            'num_pending' => 5,
            'num_ack_pending' => 2,
            'num_redelivered' => 1,
            'num_waiting' => 0,
            'created' => '2025-01-15T10:00:00Z',
            'cluster' => [
                'name' => 'test-cluster',
                'leader' => 'node-1',
                'replicas' => [],
            ],
        ];

        $info = ConsumerInfo::fromArray($data);

        self::assertSame('TEST_STREAM', $info->stream);
        self::assertSame('test-consumer', $info->name);
        self::assertSame(5, $info->numPending);
        self::assertSame(2, $info->numAckPending);
        self::assertSame(1, $info->numRedelivered);
        self::assertNotNull($info->cluster);
        self::assertInstanceOf(ClusterInfo::class, $info->cluster);
        self::assertSame('test-cluster', $info->cluster->name);
        self::assertSame('node-1', $info->cluster->leader);
    }

    public function testFromArrayWithoutCluster(): void
    {
        $data = [
            'stream_name' => 'TEST_STREAM',
            'name' => 'no-cluster-consumer',
            'config' => [
                'ack_policy' => 'explicit',
                'deliver_policy' => 'all',
            ],
            'num_pending' => 0,
        ];

        $info = ConsumerInfo::fromArray($data);

        self::assertSame('no-cluster-consumer', $info->name);
        self::assertNull($info->cluster);
    }

    public function testFromArrayMinimal(): void
    {
        $info = ConsumerInfo::fromArray([]);

        self::assertSame('', $info->stream);
        self::assertSame('', $info->name);
        self::assertSame(0, $info->numPending);
        self::assertSame(0, $info->numAckPending);
        self::assertSame(0, $info->numRedelivered);
        self::assertSame(0, $info->numWaiting);
        self::assertNull($info->created);
        self::assertFalse($info->pushBound);
        self::assertFalse($info->paused);
        self::assertNull($info->pauseRemaining);
        self::assertNull($info->cluster);
    }

    public function testFromArrayWithPauseFields(): void
    {
        $data = [
            'stream_name' => 'PAUSED_STREAM',
            'name' => 'paused-consumer',
            'config' => [],
            'paused' => true,
            'pause_remaining' => 5_000_000_000, // 5 seconds in nanoseconds
        ];

        $info = ConsumerInfo::fromArray($data);

        self::assertTrue($info->paused);
        self::assertSame(5.0, $info->pauseRemaining);
    }
}
