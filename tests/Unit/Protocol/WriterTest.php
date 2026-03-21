<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\Protocol;

use Nats\Protocol\Writer;
use PHPUnit\Framework\TestCase;

final class WriterTest extends TestCase
{
    private Writer $writer;

    protected function setUp(): void
    {
        $this->writer = new Writer();
    }

    public function testConnect(): void
    {
        $result = $this->writer->connect(['verbose' => false, 'lang' => 'php']);
        self::assertStringStartsWith('CONNECT ', $result);
        self::assertStringEndsWith("\r\n", $result);
        self::assertStringContainsString('"verbose":false', $result);
        self::assertStringContainsString('"lang":"php"', $result);
    }

    public function testPubSimple(): void
    {
        $result = $this->writer->pub('foo', 'hello');
        self::assertSame("PUB foo 5\r\nhello\r\n", $result);
    }

    public function testPubEmpty(): void
    {
        $result = $this->writer->pub('foo', '');
        self::assertSame("PUB foo 0\r\n\r\n", $result);
    }

    public function testPubWithReply(): void
    {
        $result = $this->writer->pub('foo', 'hello', '_INBOX.123');
        self::assertSame("PUB foo _INBOX.123 5\r\nhello\r\n", $result);
    }

    public function testHpub(): void
    {
        $headers = "NATS/1.0\r\nX-Test: val\r\n\r\n";
        $payload = "body";
        $result = $this->writer->hpub('foo', $headers, $payload);

        $headerBytes = strlen($headers);
        $totalBytes = $headerBytes + strlen($payload);
        self::assertSame("HPUB foo {$headerBytes} {$totalBytes}\r\n{$headers}{$payload}\r\n", $result);
    }

    public function testHpubWithReply(): void
    {
        $headers = "NATS/1.0\r\n\r\n";
        $payload = "data";
        $result = $this->writer->hpub('foo', $headers, $payload, 'reply.to');

        $headerBytes = strlen($headers);
        $totalBytes = $headerBytes + strlen($payload);
        self::assertSame("HPUB foo reply.to {$headerBytes} {$totalBytes}\r\n{$headers}{$payload}\r\n", $result);
    }

    public function testSub(): void
    {
        self::assertSame("SUB foo 1\r\n", $this->writer->sub('foo', '1'));
    }

    public function testSubWithQueue(): void
    {
        self::assertSame("SUB foo workers 1\r\n", $this->writer->sub('foo', '1', 'workers'));
    }

    public function testUnsub(): void
    {
        self::assertSame("UNSUB 1\r\n", $this->writer->unsub('1'));
    }

    public function testUnsubWithMax(): void
    {
        self::assertSame("UNSUB 1 5\r\n", $this->writer->unsub('1', 5));
    }

    public function testPing(): void
    {
        self::assertSame("PING\r\n", $this->writer->ping());
    }

    public function testPong(): void
    {
        self::assertSame("PONG\r\n", $this->writer->pong());
    }
}
