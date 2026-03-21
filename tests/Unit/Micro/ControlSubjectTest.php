<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\Micro;

use Nats\Enum\ServiceVerb;
use Nats\Micro\Service;
use PHPUnit\Framework\TestCase;

final class ControlSubjectTest extends TestCase
{
    public function testPingNoNameNoId(): void
    {
        self::assertSame('$SRV.PING', Service::controlSubject(ServiceVerb::Ping));
    }

    public function testPingWithName(): void
    {
        self::assertSame('$SRV.PING.myservice', Service::controlSubject(ServiceVerb::Ping, 'myservice'));
    }

    public function testPingWithNameAndId(): void
    {
        self::assertSame('$SRV.PING.myservice.abc123', Service::controlSubject(ServiceVerb::Ping, 'myservice', 'abc123'));
    }

    public function testInfoNoName(): void
    {
        self::assertSame('$SRV.INFO', Service::controlSubject(ServiceVerb::Info));
    }

    public function testInfoWithName(): void
    {
        self::assertSame('$SRV.INFO.svc', Service::controlSubject(ServiceVerb::Info, 'svc'));
    }

    public function testStatsWithNameAndId(): void
    {
        self::assertSame('$SRV.STATS.svc.id1', Service::controlSubject(ServiceVerb::Stats, 'svc', 'id1'));
    }

    public function testIdWithoutNameIsIgnored(): void
    {
        // If name is empty, id should be ignored
        self::assertSame('$SRV.STATS', Service::controlSubject(ServiceVerb::Stats, '', 'someid'));
    }
}
