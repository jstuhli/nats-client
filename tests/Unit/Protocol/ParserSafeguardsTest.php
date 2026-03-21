<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\Protocol;

use Nats\NatsException;
use Nats\Protocol\Command;
use Nats\Protocol\Parser;
use PHPUnit\Framework\TestCase;

/**
 * Tests for parser safeguards matching Go client behavior:
 * - MAX_CONTROL_LINE_SIZE (4096 bytes)
 * - Negative payload size rejection
 * - Header size validation (hdr < 0 || hdr > total)
 * - Max payload enforcement on inbound messages
 */
final class ParserSafeguardsTest extends TestCase
{
    private Parser $parser;

    protected function setUp(): void
    {
        $this->parser = new Parser();
    }

    // --- Control line size limits ---

    public function testRejectsControlLineExceedingMaxSize(): void
    {
        // Build a control line longer than 4096 bytes
        $longSubject = str_repeat('a', 5000);
        $line = "MSG {$longSubject} 1 0\r\n\r\n";

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Maximum control line size exceeded');
        $this->parser->parse($line);
    }

    public function testRejectsControlLineExceedingMaxSizeWithoutCrlf(): void
    {
        // Feed a buffer > 4096 bytes with no CRLF — simulates incomplete oversized line
        $data = str_repeat('X', 5000);

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Maximum control line size exceeded');
        $this->parser->parse($data);
    }

    public function testAcceptsControlLineAtMaxSize(): void
    {
        // A control line of exactly 4096 bytes should be accepted
        // "PING" = 4 chars, we need the line to be <= 4096
        $ops = $this->parser->parse("PING\r\n");
        self::assertCount(1, $ops);
        self::assertSame(Command::Ping, $ops[0]->command);
    }

    public function testAcceptsLargeButValidControlLine(): void
    {
        // Subject just under 4096 - overhead for "MSG <subject> 1 0"
        $subject = str_repeat('a', 4000);
        $line = "MSG {$subject} 1 0\r\n\r\n";

        $ops = $this->parser->parse($line);
        self::assertCount(1, $ops);
        self::assertSame($subject, $ops[0]->subject);
    }

    // --- Negative size validation (MSG) ---

    public function testRejectsNegativeSizeInMsg(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Bad or missing size in MSG');
        $this->parser->parse("MSG foo 1 -5\r\n");
    }

    public function testAcceptsZeroSizeMsg(): void
    {
        $ops = $this->parser->parse("MSG foo 1 0\r\n\r\n");
        self::assertCount(1, $ops);
        self::assertSame('', $ops[0]->payload);
        self::assertSame(0, $ops[0]->totalBytes);
    }

    // --- Negative size validation (HMSG) ---

    public function testRejectsNegativeTotalSizeInHmsg(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Bad or missing size in HMSG');
        $this->parser->parse("HMSG foo 1 10 -5\r\n");
    }

    public function testRejectsNegativeHeaderSizeInHmsg(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Bad or missing header size in HMSG');
        $this->parser->parse("HMSG foo 1 -1 10\r\n");
    }

    public function testRejectsHeaderSizeGreaterThanTotalInHmsg(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Bad or missing header size in HMSG');
        $this->parser->parse("HMSG foo 1 100 50\r\n");
    }

    public function testAcceptsHeaderSizeEqualToTotal(): void
    {
        // Headers only, no body — hdr == total is valid (empty body)
        $headers = "NATS/1.0\r\n\r\n";
        $hdrLen = strlen($headers);
        $data = "HMSG foo 1 {$hdrLen} {$hdrLen}\r\n{$headers}\r\n";

        $ops = $this->parser->parse($data);
        self::assertCount(1, $ops);
        self::assertSame(Command::HMsg, $ops[0]->command);
        self::assertSame($hdrLen, $ops[0]->headerBytes);
        self::assertSame($hdrLen, $ops[0]->totalBytes);
    }

    // --- Max payload enforcement on inbound ---

    public function testRejectsInboundMsgExceedingMaxPayload(): void
    {
        $this->parser->setMaxPayload(100);

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('exceeds max payload');
        $this->parser->parse("MSG foo 1 200\r\n");
    }

    public function testRejectsInboundHmsgExceedingMaxPayload(): void
    {
        $this->parser->setMaxPayload(100);

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('exceeds max payload');
        $this->parser->parse("HMSG foo 1 10 200\r\n");
    }

    public function testAcceptsInboundMsgWithinMaxPayload(): void
    {
        $this->parser->setMaxPayload(100);

        $payload = str_repeat('x', 50);
        $data = "MSG foo 1 50\r\n{$payload}\r\n";

        $ops = $this->parser->parse($data);
        self::assertCount(1, $ops);
        self::assertSame($payload, $ops[0]->payload);
    }

    public function testAcceptsInboundMsgAtExactMaxPayload(): void
    {
        $this->parser->setMaxPayload(100);

        $payload = str_repeat('x', 100);
        $data = "MSG foo 1 100\r\n{$payload}\r\n";

        $ops = $this->parser->parse($data);
        self::assertCount(1, $ops);
        self::assertSame(100, $ops[0]->totalBytes);
    }

    public function testMaxPayloadZeroMeansNoLimit(): void
    {
        // Default is 0 = no limit
        $payload = str_repeat('x', 10000);
        $data = "MSG foo 1 10000\r\n{$payload}\r\n";

        $ops = $this->parser->parse($data);
        self::assertCount(1, $ops);
        self::assertSame(10000, $ops[0]->totalBytes);
    }

    public function testSetMaxPayloadUpdatesLimit(): void
    {
        // First set a small limit
        $this->parser->setMaxPayload(50);

        // Msg of 100 should fail
        $threw = false;
        try {
            $this->parser->parse("MSG foo 1 100\r\n");
        } catch (NatsException) {
            $threw = true;
        }
        self::assertTrue($threw);

        // Reset parser and raise limit
        $this->parser->reset();
        $this->parser->setMaxPayload(200);

        // Now 100 should succeed
        $payload = str_repeat('x', 100);
        $data = "MSG foo 1 100\r\n{$payload}\r\n";
        $ops = $this->parser->parse($data);
        self::assertCount(1, $ops);
    }

    // --- Invalid argument counts ---

    public function testRejectsMsgWithTooFewArgs(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid MSG args');
        $this->parser->parse("MSG foo 1\r\n");
    }

    public function testRejectsMsgWithTooManyArgs(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid MSG args');
        $this->parser->parse("MSG foo 1 reply extra 5\r\n");
    }

    public function testRejectsHmsgWithTooFewArgs(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid HMSG args');
        $this->parser->parse("HMSG foo 1 10\r\n");
    }

    public function testRejectsHmsgWithTooManyArgs(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Invalid HMSG args');
        $this->parser->parse("HMSG foo 1 reply 10 20 extra\r\n");
    }

    // --- Unknown command ---

    public function testRejectsUnknownCommand(): void
    {
        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('Unknown protocol command');
        $this->parser->parse("BOGUS data\r\n");
    }

    // --- Partial accumulation still works with safeguards ---

    public function testPartialAccumulationWithMaxPayloadSet(): void
    {
        $this->parser->setMaxPayload(1000);

        $fullPayload = 'partial_data_here!!';
        $len = strlen($fullPayload); // 19

        // Send control line + partial payload
        $ops = $this->parser->parse("MSG foo 1 {$len}\r\npartial");
        self::assertCount(0, $ops);

        // Complete the payload
        $ops = $this->parser->parse("_data_here!!\r\n");
        self::assertCount(1, $ops);
        self::assertSame($fullPayload, $ops[0]->payload);
    }
}
