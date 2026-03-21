<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Stream;

use Nats\JetStream\Stream\ClusterInfo;
use Nats\JetStream\Stream\PeerInfo;
use PHPUnit\Framework\TestCase;

final class ClusterInfoTest extends TestCase
{
    public function testFromArrayWithReplicas(): void
    {
        $data = [
            'name' => 'nats-cluster',
            'leader' => 'node-1',
            'replicas' => [
                [
                    'name' => 'node-2',
                    'current' => true,
                    'offline' => false,
                    'active' => 1_500_000_000,
                    'lag' => 5,
                ],
                [
                    'name' => 'node-3',
                    'current' => false,
                    'offline' => true,
                    'active' => 3_000_000_000,
                    'lag' => 100,
                ],
            ],
        ];

        $cluster = ClusterInfo::fromArray($data);

        self::assertSame('nats-cluster', $cluster->name);
        self::assertSame('node-1', $cluster->leader);
        self::assertCount(2, $cluster->replicas);

        self::assertInstanceOf(PeerInfo::class, $cluster->replicas[0]);
        self::assertSame('node-2', $cluster->replicas[0]->name);
        self::assertTrue($cluster->replicas[0]->current);
        self::assertFalse($cluster->replicas[0]->offline);
        self::assertSame(1.5, $cluster->replicas[0]->active);
        self::assertSame(5, $cluster->replicas[0]->lag);

        self::assertSame('node-3', $cluster->replicas[1]->name);
        self::assertFalse($cluster->replicas[1]->current);
        self::assertTrue($cluster->replicas[1]->offline);
        self::assertSame(3.0, $cluster->replicas[1]->active);
        self::assertSame(100, $cluster->replicas[1]->lag);
    }

    public function testFromArrayDefaults(): void
    {
        $cluster = ClusterInfo::fromArray([]);

        self::assertSame('', $cluster->name);
        self::assertSame('', $cluster->leader);
        self::assertEmpty($cluster->replicas);
    }

    public function testFromArrayWithoutReplicas(): void
    {
        $cluster = ClusterInfo::fromArray([
            'name' => 'solo',
            'leader' => 'node-1',
        ]);

        self::assertSame('solo', $cluster->name);
        self::assertSame('node-1', $cluster->leader);
        self::assertEmpty($cluster->replicas);
    }
}
