<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamConsumerLimits
{
    public function __construct(
        public ?float $inactiveThreshold = null,
        public ?int $maxAckPending = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];
        if ($this->inactiveThreshold !== null) {
            $data['inactive_threshold'] = (int) ($this->inactiveThreshold * 1_000_000_000);
        }
        if ($this->maxAckPending !== null) {
            $data['max_ack_pending'] = $this->maxAckPending;
        }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            inactiveThreshold: isset($data['inactive_threshold']) ? T::int($data['inactive_threshold']) / 1_000_000_000 : null,
            maxAckPending: T::nullableInt($data['max_ack_pending'] ?? null),
        );
    }
}
