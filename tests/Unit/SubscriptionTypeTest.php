<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Subscription;
use PHPUnit\Framework\TestCase;

final class SubscriptionTypeTest extends TestCase
{
    private function createSub(?\Closure $handler): Subscription
    {
        $ref = new \ReflectionClass(Subscription::class);
        $sub = $ref->newInstanceWithoutConstructor();

        $conn = (new \ReflectionClass(\Nats\Connection::class))->newInstanceWithoutConstructor();

        foreach ([
            'connection' => $conn,
            'sid' => '1',
            'subject' => 'test',
            'queue' => null,
            'handler' => $handler,
            'messageBuffer' => [],
            'delivered' => 0,
            'dropped' => 0,
            'maxMessages' => 0,
            'closed' => false,
            'draining' => false,
            'pendingMsgLimit' => 500_000,
            'pendingBytesLimit' => 67_108_864,
            'pendingBytes' => 0,
            'maxPendingMsgs' => 0,
            'maxPendingBytes' => 0,
            'closedHandler' => null,
        ] as $name => $value) {
            $prop = $ref->getProperty($name);
            $prop->setValue($sub, $value);
        }

        return $sub;
    }

    public function testSyncSubscriptionType(): void
    {
        $sub = $this->createSub(null);
        self::assertSame('sync', $sub->type());
    }

    public function testAsyncSubscriptionType(): void
    {
        $sub = $this->createSub(function (): void {});
        self::assertSame('async', $sub->type());
    }
}
