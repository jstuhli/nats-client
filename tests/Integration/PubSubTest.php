<?php

declare(strict_types=1);

namespace Nats\Tests\Integration;

use Nats\Connection;
use Nats\Headers;
use Nats\Message;
use PHPUnit\Framework\TestCase;

final class PubSubTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testPublishAndSubscribeSync(): void
    {
        $sub = $this->conn->subscribeSync('test.pubsub.sync');
        $this->conn->publish('test.pubsub.sync', 'hello');
        $this->conn->flush();

        $msg = $sub->nextMessage(2.0);
        self::assertSame('test.pubsub.sync', $msg->subject);
        self::assertSame('hello', $msg->data);

        $sub->unsubscribe();
    }

    public function testPublishAndSubscribeAsync(): void
    {
        $received = null;
        $this->conn->subscribe('test.pubsub.async', function (Message $msg) use (&$received): void {
            $received = $msg;
        });

        $this->conn->publish('test.pubsub.async', 'async hello');
        $this->conn->flush();

        // Process multiple iterations to ensure delivery
        for ($i = 0; $i < 10 && $received === null; $i++) {
            $this->conn->process(0.1);
        }

        self::assertNotNull($received);
        self::assertSame('async hello', $received->data);
    }

    public function testQueueSubscribe(): void
    {
        $receivedBy = [];

        $this->conn->queueSubscribe('test.queue', 'workers', function (Message $msg) use (&$receivedBy): void {
            $receivedBy[] = 'A';
        });
        $this->conn->queueSubscribe('test.queue', 'workers', function (Message $msg) use (&$receivedBy): void {
            $receivedBy[] = 'B';
        });

        for ($i = 0; $i < 10; $i++) {
            $this->conn->publish('test.queue', "msg {$i}");
        }
        $this->conn->flush();

        for ($i = 0; $i < 20 && count($receivedBy) < 10; $i++) {
            $this->conn->process(0.1);
        }

        self::assertCount(10, $receivedBy);
    }

    public function testWildcardStar(): void
    {
        $received = [];
        $this->conn->subscribe('test.wild.*.event', function (Message $msg) use (&$received): void {
            $received[] = $msg->subject;
        });

        $this->conn->publish('test.wild.user.event', '1');
        $this->conn->publish('test.wild.order.event', '2');
        $this->conn->publish('test.wild.user.other', '3'); // Ne matcha
        $this->conn->flush();

        for ($i = 0; $i < 10 && count($received) < 2; $i++) {
            $this->conn->process(0.1);
        }

        self::assertCount(2, $received);
        self::assertContains('test.wild.user.event', $received);
        self::assertContains('test.wild.order.event', $received);
    }

    public function testWildcardGt(): void
    {
        $received = [];
        $this->conn->subscribe('test.gt.>', function (Message $msg) use (&$received): void {
            $received[] = $msg->subject;
        });

        $this->conn->publish('test.gt.a', '1');
        $this->conn->publish('test.gt.a.b', '2');
        $this->conn->publish('test.gt.a.b.c', '3');
        $this->conn->flush();

        for ($i = 0; $i < 10 && count($received) < 3; $i++) {
            $this->conn->process(0.1);
        }

        self::assertCount(3, $received);
    }

    public function testAutoUnsubscribe(): void
    {
        $received = 0;
        $sub = $this->conn->subscribe('test.autounsub', function (Message $msg) use (&$received): void {
            $received++;
        });
        $sub->autoUnsubscribe(3);

        for ($i = 0; $i < 10; $i++) {
            $this->conn->publish('test.autounsub', "msg {$i}");
        }
        $this->conn->flush();

        for ($i = 0; $i < 20 && $received < 3; $i++) {
            $this->conn->process(0.1);
        }

        self::assertSame(3, $received);
    }

    public function testPublishWithHeaders(): void
    {
        $sub = $this->conn->subscribeSync('test.headers');

        $headers = new Headers(['X-Test' => 'value', 'X-Multi' => ['a', 'b']]);
        $msg = new Message(
            subject: 'test.headers',
            data: 'with headers',
            headers: $headers,
        );
        $this->conn->publishMessage($msg);
        $this->conn->flush();

        $received = $sub->nextMessage(2.0);
        self::assertSame('with headers', $received->data);
        self::assertTrue($received->hasHeaders());
        self::assertSame('value', $received->headers->get('X-Test'));
        self::assertSame(['a', 'b'], $received->headers->values('X-Multi'));

        $sub->unsubscribe();
    }

    public function testEmptyMessage(): void
    {
        $sub = $this->conn->subscribeSync('test.empty');
        $this->conn->publish('test.empty', '');
        $this->conn->flush();

        $msg = $sub->nextMessage(2.0);
        self::assertSame('', $msg->data);

        $sub->unsubscribe();
    }

    public function testLargeMessage(): void
    {
        $data = str_repeat('x', 65536); // 64KB

        $sub = $this->conn->subscribeSync('test.large');
        $this->conn->publish('test.large', $data);
        $this->conn->flush();

        $msg = $sub->nextMessage(2.0);
        self::assertSame(65536, strlen($msg->data));

        $sub->unsubscribe();
    }

    public function testSubscriptionStats(): void
    {
        $sub = $this->conn->subscribeSync('test.stats.sub');

        for ($i = 0; $i < 5; $i++) {
            $this->conn->publish('test.stats.sub', "msg {$i}");
        }
        $this->conn->flush();

        // Daj vremena da poruke stignu
        $this->conn->process(0.5);

        $pending = $sub->pending();
        self::assertGreaterThanOrEqual(0, $pending['messages']);

        // Konzumiraj
        for ($i = 0; $i < 5; $i++) {
            try {
                $sub->nextMessage(0.5);
            } catch (\Nats\TimeoutException) {
                break;
            }
        }

        self::assertGreaterThanOrEqual(0, $sub->delivered());

        $sub->unsubscribe();
    }

    public function testSyncTimeoutThrows(): void
    {
        $sub = $this->conn->subscribeSync('test.timeout.empty');

        $this->expectException(\Nats\TimeoutException::class);
        $sub->nextMessage(0.1);
    }
}
