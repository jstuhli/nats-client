<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Internal\TypeCast as T;

readonly class ConsumerPauseResponse
{
    public function __construct(
        public bool $paused,
        public ?\DateTimeImmutable $pauseUntil = null,
        public ?float $pauseRemaining = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            paused: T::bool($data['paused'] ?? false),
            pauseUntil: isset($data['pause_until']) ? new \DateTimeImmutable(T::string($data['pause_until'])) : null,
            pauseRemaining: isset($data['pause_remaining']) ? T::int($data['pause_remaining']) / 1_000_000_000 : null,
        );
    }
}
