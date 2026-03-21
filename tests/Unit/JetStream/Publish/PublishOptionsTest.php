<?php

declare(strict_types=1);

namespace Nats\Tests\Unit\JetStream\Publish;

use Nats\Headers;
use Nats\JetStream\Publish\PublishOptions;
use PHPUnit\Framework\TestCase;

final class PublishOptionsTest extends TestCase
{
    public function testMsgId(): void
    {
        $opt = PublishOptions::msgId('unique-123');

        self::assertSame('msg_id', $opt->type);
        self::assertSame('unique-123', $opt->value);
    }

    public function testMsgTtl(): void
    {
        $opt = PublishOptions::msgTtl(60.0);

        self::assertSame('msg_ttl', $opt->type);
        self::assertSame(60.0, $opt->value);
    }

    public function testExpectStream(): void
    {
        $opt = PublishOptions::expectStream('orders');

        self::assertSame('expect_stream', $opt->type);
        self::assertSame('orders', $opt->value);
    }

    public function testExpectLastSequence(): void
    {
        $opt = PublishOptions::expectLastSequence(42);

        self::assertSame('expect_last_sequence', $opt->type);
        self::assertSame(42, $opt->value);
    }

    public function testExpectLastSequencePerSubject(): void
    {
        $opt = PublishOptions::expectLastSequencePerSubject(10);

        self::assertSame('expect_last_sequence_per_subject', $opt->type);
        self::assertSame(10, $opt->value);
    }

    public function testExpectLastMsgId(): void
    {
        $opt = PublishOptions::expectLastMsgId('msg-99');

        self::assertSame('expect_last_msg_id', $opt->type);
        self::assertSame('msg-99', $opt->value);
    }

    public function testExpectLastSequenceForSubject(): void
    {
        $opt = PublishOptions::expectLastSequenceForSubject(5, 'orders.new');

        self::assertSame('expect_last_sequence_for_subject', $opt->type);
        self::assertSame(['seq' => 5, 'subject' => 'orders.new'], $opt->value);
    }

    public function testStallWait(): void
    {
        $opt = PublishOptions::stallWait(2.5);

        self::assertSame('stall_wait', $opt->type);
        self::assertSame(2.5, $opt->value);
    }

    public function testRetryAttempts(): void
    {
        $opt = PublishOptions::retryAttempts(3);

        self::assertSame('retry_attempts', $opt->type);
        self::assertSame(3, $opt->value);
    }

    public function testRetryWait(): void
    {
        $opt = PublishOptions::retryWait(1.0);

        self::assertSame('retry_wait', $opt->type);
        self::assertSame(1.0, $opt->value);
    }

    public function testApplyToHeadersMsgId(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::msgId('abc')]);

        self::assertSame('abc', $headers->get('Nats-Msg-Id'));
    }

    public function testApplyToHeadersMsgTtl(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::msgTtl(5.0)]);

        self::assertSame((string) 5_000_000_000, $headers->get('Nats-Msg-Ttl'));
    }

    public function testApplyToHeadersExpectStream(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::expectStream('mystream')]);

        self::assertSame('mystream', $headers->get('Nats-Expected-Stream'));
    }

    public function testApplyToHeadersExpectLastSequence(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::expectLastSequence(99)]);

        self::assertSame('99', $headers->get('Nats-Expected-Last-Sequence'));
    }

    public function testApplyToHeadersExpectLastSequencePerSubject(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::expectLastSequencePerSubject(7)]);

        self::assertSame('7', $headers->get('Nats-Expected-Last-Subject-Sequence'));
    }

    public function testApplyToHeadersExpectLastMsgId(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [PublishOptions::expectLastMsgId('prev-id')]);

        self::assertSame('prev-id', $headers->get('Nats-Expected-Last-Msg-Id'));
    }

    public function testApplyToHeadersExpectLastSequenceForSubject(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [
            PublishOptions::expectLastSequenceForSubject(12, 'orders.created'),
        ]);

        self::assertSame('12', $headers->get('Nats-Expected-Last-Subject-Sequence'));
    }

    public function testApplyToHeadersMultipleOptions(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [
            PublishOptions::msgId('id-1'),
            PublishOptions::expectStream('events'),
            PublishOptions::expectLastSequence(50),
        ]);

        self::assertSame('id-1', $headers->get('Nats-Msg-Id'));
        self::assertSame('events', $headers->get('Nats-Expected-Stream'));
        self::assertSame('50', $headers->get('Nats-Expected-Last-Sequence'));
    }

    public function testApplyToHeadersStallWaitAndRetryDoNotSetHeaders(): void
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, [
            PublishOptions::stallWait(1.0),
            PublishOptions::retryAttempts(3),
            PublishOptions::retryWait(0.5),
        ]);

        self::assertSame(0, $headers->count());
    }
}
