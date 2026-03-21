<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Headers;
use Nats\Message;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    public function testConstruction(): void
    {
        $msg = new Message(subject: 'test.subject', data: 'hello');

        self::assertSame('test.subject', $msg->subject);
        self::assertSame('hello', $msg->data);
        self::assertNull($msg->replyTo);
        self::assertNull($msg->headers);
    }

    public function testWithAllFields(): void
    {
        $headers = new Headers(['X-Test' => 'val']);
        $msg = new Message(
            subject: 'foo.bar',
            data: 'payload',
            replyTo: '_INBOX.123',
            headers: $headers,
        );

        self::assertSame('foo.bar', $msg->subject);
        self::assertSame('payload', $msg->data);
        self::assertSame('_INBOX.123', $msg->replyTo);
        self::assertSame('val', $msg->headers->get('X-Test'));
    }

    public function testSize(): void
    {
        $msg = new Message(subject: 'test', data: 'hello');
        self::assertSame(5, $msg->size());
    }

    public function testSizeWithHeaders(): void
    {
        $headers = new Headers(['X-Test' => 'val']);
        $msg = new Message(subject: 'test', data: 'hello', headers: $headers);

        $expectedSize = strlen('hello') + strlen($headers->toWireFormat());
        self::assertSame($expectedSize, $msg->size());
    }

    public function testHasHeaders(): void
    {
        $msg1 = new Message(subject: 'test', data: '');
        self::assertFalse($msg1->hasHeaders());

        $msg2 = new Message(subject: 'test', data: '', headers: new Headers(['X' => 'y']));
        self::assertTrue($msg2->hasHeaders());
    }

    public function testRespondWithoutReplyThrows(): void
    {
        $msg = new Message(subject: 'test', data: '');

        $this->expectException(\Nats\NatsException::class);
        $this->expectExceptionMessage('No reply subject');
        $msg->respond('data');
    }
}
