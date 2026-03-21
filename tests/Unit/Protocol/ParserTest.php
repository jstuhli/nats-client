<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\Protocol;

use Nats\Protocol\Command;
use Nats\Protocol\Parser;
use PHPUnit\Framework\TestCase;

final class ParserTest extends TestCase
{
    private Parser $parser;

    protected function setUp(): void
    {
        $this->parser = new Parser();
    }

    public function testParseInfo(): void
    {
        $data = "INFO {\"server_id\":\"test\",\"version\":\"2.10.0\"}\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame(Command::Info, $ops[0]->command);
        self::assertStringContainsString('server_id', $ops[0]->payload);
    }

    public function testParsePing(): void
    {
        $ops = $this->parser->parse("PING\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Ping, $ops[0]->command);
    }

    public function testParsePong(): void
    {
        $ops = $this->parser->parse("PONG\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Pong, $ops[0]->command);
    }

    public function testParseOk(): void
    {
        $ops = $this->parser->parse("+OK\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Ok, $ops[0]->command);
    }

    public function testParseErr(): void
    {
        $ops = $this->parser->parse("-ERR 'Authorization Violation'\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Err, $ops[0]->command);
        self::assertSame('Authorization Violation', $ops[0]->payload);
    }

    public function testParseMsgWithoutReply(): void
    {
        $payload = "hello world";
        $data = "MSG foo.bar 1 11\r\n{$payload}\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame(Command::Msg, $ops[0]->command);
        self::assertSame('foo.bar', $ops[0]->subject);
        self::assertSame('1', $ops[0]->sid);
        self::assertNull($ops[0]->replyTo);
        self::assertSame(11, $ops[0]->totalBytes);
        self::assertSame($payload, $ops[0]->payload);
    }

    public function testParseMsgWithReply(): void
    {
        $payload = "test";
        $data = "MSG foo.bar 1 _INBOX.abc 4\r\n{$payload}\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame(Command::Msg, $ops[0]->command);
        self::assertSame('foo.bar', $ops[0]->subject);
        self::assertSame('1', $ops[0]->sid);
        self::assertSame('_INBOX.abc', $ops[0]->replyTo);
        self::assertSame($payload, $ops[0]->payload);
    }

    public function testParseHMsg(): void
    {
        $headers = "NATS/1.0\r\nX-Test: value\r\n\r\n";
        $body = "body";
        $headerBytes = strlen($headers);
        $totalBytes = $headerBytes + strlen($body);

        $data = "HMSG foo.bar 1 {$headerBytes} {$totalBytes}\r\n{$headers}{$body}\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame(Command::HMsg, $ops[0]->command);
        self::assertSame('foo.bar', $ops[0]->subject);
        self::assertSame($headerBytes, $ops[0]->headerBytes);
        self::assertSame($totalBytes, $ops[0]->totalBytes);
        self::assertSame($headers . $body, $ops[0]->payload);
    }

    public function testParseHMsgWithReply(): void
    {
        $headers = "NATS/1.0\r\n\r\n";
        $body = "data";
        $headerBytes = strlen($headers);
        $totalBytes = $headerBytes + strlen($body);

        $data = "HMSG subject 2 reply.to {$headerBytes} {$totalBytes}\r\n{$headers}{$body}\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame('reply.to', $ops[0]->replyTo);
    }

    public function testParseMultipleOps(): void
    {
        $data = "PING\r\nPONG\r\n+OK\r\n";
        $ops = $this->parser->parse($data);
        self::assertCount(3, $ops);
    }

    public function testParsePartialData(): void
    {
        // Send control line but not full payload
        $ops = $this->parser->parse("MSG foo 1 5\r\nhel");
        self::assertCount(0, $ops);

        // Send rest of payload
        $ops = $this->parser->parse("lo\r\n");
        self::assertCount(1, $ops);
        self::assertSame('hello', $ops[0]->payload);
    }

    public function testParsePartialControlLine(): void
    {
        $ops = $this->parser->parse("PIN");
        self::assertCount(0, $ops);

        $ops = $this->parser->parse("G\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Ping, $ops[0]->command);
    }

    public function testParseEmptyPayload(): void
    {
        $data = "MSG foo 1 0\r\n\r\n";
        $ops = $this->parser->parse($data);

        self::assertCount(1, $ops);
        self::assertSame('', $ops[0]->payload);
    }

    public function testReset(): void
    {
        $this->parser->parse("MSG foo 1 5\r\nhel");
        $this->parser->reset();

        $ops = $this->parser->parse("PING\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Ping, $ops[0]->command);
    }
}
