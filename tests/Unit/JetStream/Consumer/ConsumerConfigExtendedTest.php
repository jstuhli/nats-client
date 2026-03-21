<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Consumer;

use Nats\JetStream\Consumer\ConsumerConfig;
use PHPUnit\Framework\TestCase;

final class ConsumerConfigExtendedTest extends TestCase
{
    public function testPriorityPolicy(): void
    {
        $config = new ConsumerConfig(priorityPolicy: 'overflow');
        $array = $config->toArray();

        self::assertSame('overflow', $array['priority_policy']);

        $restored = ConsumerConfig::fromArray($array);
        self::assertSame('overflow', $restored->priorityPolicy);
    }

    public function testPinnedTtl(): void
    {
        $config = new ConsumerConfig(pinnedTtl: 5.0);
        $array = $config->toArray();

        // Should be stored as nanoseconds in the wire format
        self::assertSame(5_000_000_000, $array['pinned_ttl']);

        // Round-trip: fromArray should convert nanoseconds back to seconds
        $restored = ConsumerConfig::fromArray($array);
        self::assertSame(5.0, $restored->pinnedTtl);
    }

    public function testPinnedTtlFractionalSeconds(): void
    {
        $config = new ConsumerConfig(pinnedTtl: 1.5);
        $array = $config->toArray();

        self::assertSame(1_500_000_000, $array['pinned_ttl']);

        $restored = ConsumerConfig::fromArray($array);
        self::assertSame(1.5, $restored->pinnedTtl);
    }

    public function testPriorityGroups(): void
    {
        $groups = ['high', 'medium', 'low'];
        $config = new ConsumerConfig(priorityGroups: $groups);
        $array = $config->toArray();

        self::assertSame($groups, $array['priority_groups']);

        $restored = ConsumerConfig::fromArray($array);
        self::assertSame($groups, $restored->priorityGroups);
    }

    public function testAllNewFieldsTogether(): void
    {
        $config = new ConsumerConfig(
            name: 'priority-consumer',
            priorityPolicy: 'overflow',
            pinnedTtl: 10.0,
            priorityGroups: ['critical', 'normal'],
        );

        $array = $config->toArray();

        self::assertSame('priority-consumer', $array['name']);
        self::assertSame('overflow', $array['priority_policy']);
        self::assertSame(10_000_000_000, $array['pinned_ttl']);
        self::assertSame(['critical', 'normal'], $array['priority_groups']);

        $restored = ConsumerConfig::fromArray($array);

        self::assertSame('priority-consumer', $restored->name);
        self::assertSame('overflow', $restored->priorityPolicy);
        self::assertSame(10.0, $restored->pinnedTtl);
        self::assertSame(['critical', 'normal'], $restored->priorityGroups);
    }

    public function testNullPriorityFieldsOmittedFromArray(): void
    {
        $config = new ConsumerConfig();
        $array = $config->toArray();

        self::assertArrayNotHasKey('priority_policy', $array);
        self::assertArrayNotHasKey('pinned_ttl', $array);
        self::assertArrayNotHasKey('priority_groups', $array);
    }

    public function testFromArrayDefaultsForPriorityFields(): void
    {
        $config = ConsumerConfig::fromArray([]);

        self::assertNull($config->priorityPolicy);
        self::assertNull($config->pinnedTtl);
        self::assertSame([], $config->priorityGroups);
    }
}
