<?php

declare(strict_types=1);

namespace Nats\JetStream\Message;

readonly class MessageMetadata
{
    public function __construct(
        public int $streamSequence,
        public int $consumerSequence,
        public int $numPending,
        public int $numDelivered,
        public \DateTimeImmutable $timestamp,
        public string $stream,
        public string $consumer,
        public ?string $domain = null,
    ) {}

    /**
     * Parse metadata from NATS reply subject.
     * Format: $JS.ACK.<domain>.<account-hash>.<stream>.<consumer>.<num-delivered>.<stream-seq>.<consumer-seq>.<timestamp>.<num-pending>.<uptotoken>
     */
    public static function fromReplySubject(string $replyTo): self
    {
        $parts = explode('.', $replyTo);

        // Minimum: $JS.ACK.<stream>.<consumer>.<delivered>.<stream-seq>.<consumer-seq>.<ts>.<pending>
        $partCount = count($parts);
        if ($partCount < 9) {
            return new self(
                streamSequence: 0,
                consumerSequence: 0,
                numPending: 0,
                numDelivered: 0,
                timestamp: new \DateTimeImmutable(),
                stream: '',
                consumer: '',
                domain: null,
            );
        }

        $hasDomain = count($parts) >= 12;
        $offset = $hasDomain ? 4 : 2;
        $domain = $hasDomain ? $parts[2] : null;

        $stream = $parts[$offset];
        $consumer = $parts[$offset + 1];
        $numDelivered = (int) $parts[$offset + 2];
        $streamSeq = (int) $parts[$offset + 3];
        $consumerSeq = (int) $parts[$offset + 4];
        $tsNano = (int) $parts[$offset + 5];
        $numPending = (int) $parts[$offset + 6];

        // Timestamp is in nanoseconds
        $tsSec = (int) ($tsNano / 1_000_000_000);
        $timestamp = \DateTimeImmutable::createFromFormat(
            'U',
            (string) $tsSec,
        ) ?: new \DateTimeImmutable();

        return new self(
            streamSequence: $streamSeq,
            consumerSequence: $consumerSeq,
            numPending: $numPending,
            numDelivered: $numDelivered,
            timestamp: $timestamp,
            stream: $stream,
            consumer: $consumer,
            domain: $domain,
        );
    }
}
