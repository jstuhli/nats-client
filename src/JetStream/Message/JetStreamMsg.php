<?php

declare(strict_types=1);

namespace Nats\JetStream\Message;

use Nats\Connection;
use Nats\Headers;
use Nats\Message;
use Nats\NatsException;

final class JetStreamMsg implements JetStreamMessage
{
    private bool $acked = false;
    private ?MessageMetadata $metadata = null;

    public function __construct(
        private readonly Message $msg,
        private readonly Connection $connection,
    ) {}

    public function subject(): string
    {
        return $this->msg->subject;
    }

    public function data(): string
    {
        return $this->msg->data;
    }

    public function headers(): ?Headers
    {
        return $this->msg->headers;
    }

    public function metadata(): MessageMetadata
    {
        if ($this->metadata === null) {
            if ($this->msg->replyTo === null) {
                throw new NatsException('No reply subject for metadata parsing');
            }
            $this->metadata = MessageMetadata::fromReplySubject($this->msg->replyTo);
        }
        return $this->metadata;
    }

    public function ack(): void
    {
        $this->respondAck('+ACK');
    }

    public function ackSync(float $timeout = 5.0): void
    {
        if ($this->msg->replyTo === null) {
            throw new NatsException('Cannot ack: no reply subject');
        }
        // For sync ack, use request-reply pattern
        $this->connection->request($this->msg->replyTo, '+ACK', $timeout);
        $this->acked = true;
    }

    public function nak(): void
    {
        $this->respondAck('-NAK');
    }

    public function nakWithDelay(float $seconds): void
    {
        $delayNs = (int) ($seconds * 1_000_000_000);
        $this->respondAck("-NAK {\"delay\":{$delayNs}}");
    }

    public function term(): void
    {
        $this->respondAck('+TERM');
    }

    public function termWithReason(string $reason): void
    {
        $this->respondAck("+TERM {$reason}");
    }

    public function inProgress(): void
    {
        $this->respondAck('+WPI');
    }

    public function doubleAcked(): bool
    {
        return $this->acked;
    }

    private function respondAck(string $body): void
    {
        if ($this->msg->replyTo === null) {
            throw new NatsException('Cannot ack: no reply subject');
        }
        if ($this->acked && str_starts_with($body, '+ACK')) {
            return; // Already acked, ignore duplicate
        }
        $this->connection->publish($this->msg->replyTo, $body);
        if (str_starts_with($body, '+ACK') || str_starts_with($body, '+TERM')) {
            $this->acked = true;
        }
    }

    public function raw(): Message
    {
        return $this->msg;
    }
}
