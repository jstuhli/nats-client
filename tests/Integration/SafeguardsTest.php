<?php

declare(strict_types=1);

namespace Nats\Tests\Integration;

use Nats\BadSubjectException;
use Nats\Connection;
use Nats\ConnectionOptions;
use Nats\Headers;
use Nats\MaxPayloadException;
use Nats\Message;
use Nats\NatsException;
use Nats\SlowConsumerException;
use PHPUnit\Framework\TestCase;

/**
 * Integration tests for Go-client-matching safeguards:
 * - Max payload enforcement on publish
 * - Subject validation on publish and subscribe
 * - Headers support check
 * - Slow consumer detection
 * - Pending bytes tracking through dequeue
 */
final class SafeguardsTest extends TestCase
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

    // --- Max payload on publish ---

    public function testPublishWithinMaxPayload(): void
    {
        // Small message should succeed — server default max_payload is 1MB
        $this->conn->publish('test.safeguard.small.' . uniqid(), str_repeat('x', 1000));
        $this->conn->flush();
        $this->addToAssertionCount(1);
    }

    public function testPublishExceedingMaxPayloadThrows(): void
    {
        $maxPayload = $this->conn->maxPayload();
        self::assertGreaterThan(0, $maxPayload, 'Server should report a max_payload');

        $this->expectException(MaxPayloadException::class);
        $this->expectExceptionMessage('Maximum payload exceeded');

        // Publish data larger than server's max_payload
        $this->conn->publish('test.safeguard.oversized', str_repeat('x', $maxPayload + 1));
    }

    public function testPublishMessageExceedingMaxPayloadThrows(): void
    {
        $maxPayload = $this->conn->maxPayload();

        $this->expectException(MaxPayloadException::class);

        $msg = new Message(
            subject: 'test.safeguard.oversized.msg',
            data: str_repeat('y', $maxPayload + 1),
        );
        $this->conn->publishMessage($msg);
    }

    public function testPublishMessageWithHeadersCountsHeadersInSize(): void
    {
        $maxPayload = $this->conn->maxPayload();

        // Create a message where data alone is under limit, but data + headers exceed it
        $headers = new Headers(['X-Large' => str_repeat('v', 1000)]);
        $headerWire = $headers->toWireFormat();
        $headerSize = strlen($headerWire);

        // Data size that pushes total over max_payload
        $dataSize = $maxPayload - $headerSize + 1;
        if ($dataSize < 0) {
            self::markTestSkipped('Headers alone exceed max payload');
        }

        $this->expectException(MaxPayloadException::class);

        $msg = new Message(
            subject: 'test.safeguard.hdr.oversized',
            data: str_repeat('z', $dataSize),
            headers: $headers,
        );
        $this->conn->publishMessage($msg);
    }

    public function testPublishAtExactMaxPayload(): void
    {
        $maxPayload = $this->conn->maxPayload();

        // Exactly at max_payload should succeed
        $this->conn->publish('test.safeguard.exact.' . uniqid(), str_repeat('a', $maxPayload));
        $this->conn->flush();
        $this->addToAssertionCount(1);
    }

    // --- Subject validation on publish ---

    public function testPublishRejectsEmptySubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->publish('', 'data');
    }

    public function testPublishRejectsSubjectWithSpace(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->publish('foo bar', 'data');
    }

    public function testPublishRejectsSubjectWithTab(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->publish("foo\tbar", 'data');
    }

    public function testPublishRejectsSubjectWithNewline(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->publish("foo\nbar", 'data');
    }

    public function testPublishRejectsSubjectWithCarriageReturn(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->publish("foo\rbar", 'data');
    }

    public function testPublishMessageRejectsInvalidSubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $msg = new Message(subject: 'bad subject', data: 'data');
        $this->conn->publishMessage($msg);
    }

    // --- Subject validation on subscribe ---

    public function testSubscribeRejectsEmptySubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->subscribe('', function (): void {});
    }

    public function testSubscribeRejectsSubjectWithSpace(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->subscribe('foo bar', function (): void {});
    }

    public function testSubscribeSyncRejectsInvalidSubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->subscribeSync("invalid\nsubject");
    }

    public function testQueueSubscribeRejectsInvalidSubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->queueSubscribe("bad\tsubject", 'q', function (): void {});
    }

    public function testQueueSubscribeSyncRejectsInvalidSubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->queueSubscribeSync("bad subject", 'q');
    }

    // --- Valid subjects work end-to-end ---

    public function testValidSubjectsWorkEndToEnd(): void
    {
        $subjects = [
            'simple.test.' . uniqid(),
            '$KV.test.' . uniqid(),
            '_INBOX.' . uniqid(),
            'a-b-c.' . uniqid(),
        ];

        foreach ($subjects as $subject) {
            $sub = $this->conn->subscribeSync($subject);
            $this->conn->publish($subject, 'hello');
            $this->conn->flush();
            $msg = $sub->nextMessage(2.0);
            self::assertSame('hello', $msg->data, "Failed for subject: {$subject}");
            $sub->unsubscribe();
        }
    }

    // --- Slow consumer detection ---

    public function testSlowConsumerDetection(): void
    {
        $slowConsumerReported = false;
        $slowConsumerError = null;

        $options = (new ConnectionOptions())->onError(function ($conn, $err) use (&$slowConsumerReported, &$slowConsumerError): void {
            if ($err instanceof SlowConsumerException) {
                $slowConsumerReported = true;
                $slowConsumerError = $err;
            }
        });

        $conn = Connection::connect(self::NATS_URL, $options);

        try {
            $subject = 'test.slowconsumer.' . uniqid();
            $sub = $conn->subscribeSync($subject);
            $sub->setPendingLimits(5, 0); // Only 5 messages allowed

            // Publish 10 messages to exceed the limit
            for ($i = 0; $i < 10; $i++) {
                $conn->publish($subject, "msg-{$i}");
            }
            $conn->flush();

            // Process incoming messages — some will be dropped
            $deadline = microtime(true) + 2.0;
            while (microtime(true) < $deadline) {
                $conn->process(0.05);
                if ($sub->dropped() > 0) {
                    break;
                }
            }

            self::assertGreaterThan(0, $sub->dropped(), 'Some messages should be dropped');
            self::assertTrue($slowConsumerReported, 'Slow consumer should have been reported');
            self::assertInstanceOf(SlowConsumerException::class, $slowConsumerError);
            self::assertStringContainsString($subject, $slowConsumerError->getMessage());

            $sub->unsubscribe();
        } finally {
            $conn->close();
        }
    }

    // --- Pending bytes decrease on dequeue ---

    public function testPendingBytesDecreaseOnDequeue(): void
    {
        $subject = 'test.pendingbytes.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        // Publish messages
        $this->conn->publish($subject, 'aaaa'); // 4 bytes
        $this->conn->publish($subject, 'bbbbbb'); // 6 bytes
        $this->conn->flush();

        // Wait for both messages
        $deadline = microtime(true) + 2.0;
        while ($sub->queuedMsgs() < 2 && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        $pending = $sub->pending();
        self::assertSame(2, $pending['messages']);
        self::assertSame(10, $pending['bytes']); // 4 + 6

        // Dequeue one message
        $msg1 = $sub->nextMessage(1.0);
        self::assertSame('aaaa', $msg1->data);

        $pending = $sub->pending();
        self::assertSame(1, $pending['messages']);
        self::assertSame(6, $pending['bytes']); // Only 'bbbbbb' remains

        // Dequeue the other
        $msg2 = $sub->nextMessage(1.0);
        self::assertSame('bbbbbb', $msg2->data);

        $pending = $sub->pending();
        self::assertSame(0, $pending['messages']);
        self::assertSame(0, $pending['bytes']);

        $sub->unsubscribe();
    }

    // --- Request also validates subject ---

    public function testRequestRejectsInvalidSubject(): void
    {
        $this->expectException(BadSubjectException::class);
        $this->conn->request('bad subject', 'data', 0.5);
    }

    // --- Headers support check ---

    public function testPublishMessageWithHeadersOnSupportingServer(): void
    {
        // Our test NATS server supports headers
        self::assertTrue($this->conn->headersSupported());

        $subject = 'test.headers.ok.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);

        $msg = new Message(
            subject: $subject,
            data: 'payload',
            headers: new Headers(['X-Test' => 'value']),
        );
        $this->conn->publishMessage($msg);
        $this->conn->flush();

        $received = $sub->nextMessage(2.0);
        self::assertSame('payload', $received->data);
        self::assertNotNull($received->headers);
        self::assertSame('value', $received->headers->get('X-Test'));

        $sub->unsubscribe();
    }

    // --- Connection reports max payload ---

    public function testMaxPayloadReportedFromServer(): void
    {
        $maxPayload = $this->conn->maxPayload();
        self::assertGreaterThan(0, $maxPayload);
        // Default NATS max_payload is 1MB (1048576)
        self::assertSame(1_048_576, $maxPayload);
    }

    // --- Subscription dropped counter ---

    public function testDroppedCounterAccumulates(): void
    {
        $subject = 'test.dropped.' . uniqid();
        $sub = $this->conn->subscribeSync($subject);
        $sub->setPendingLimits(2, 0);

        for ($i = 0; $i < 10; $i++) {
            $this->conn->publish($subject, "m{$i}");
        }
        $this->conn->flush();

        // Process to let messages arrive
        $deadline = microtime(true) + 2.0;
        while ($sub->delivered() < 10 && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        self::assertSame(2, $sub->queuedMsgs(), 'Only 2 messages in buffer');
        self::assertSame(8, $sub->dropped(), '8 messages should be dropped');
        self::assertSame(10, $sub->delivered(), 'All 10 counted as delivered');

        $sub->unsubscribe();
    }
}
