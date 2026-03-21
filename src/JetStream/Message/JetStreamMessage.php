<?php

declare(strict_types=1);

namespace Nats\JetStream\Message;

use Nats\Headers;

interface JetStreamMessage
{
    public function subject(): string;

    public function data(): string;

    public function headers(): ?Headers;

    public function metadata(): MessageMetadata;

    public function ack(): void;

    public function ackSync(float $timeout = 5.0): void;

    public function nak(): void;

    public function nakWithDelay(float $seconds): void;

    public function term(): void;

    public function termWithReason(string $reason): void;

    public function inProgress(): void;

    public function doubleAcked(): bool;
}
