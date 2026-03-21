<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class PeerInfo
{
    public function __construct(
        public string $name,
        public bool $current = false,
        public bool $offline = false,
        public float $active = 0,
        public int $lag = 0,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            current: T::bool($data['current'] ?? false),
            offline: T::bool($data['offline'] ?? false),
            active: isset($data['active']) ? T::int($data['active']) / 1_000_000_000 : 0,
            lag: T::int($data['lag'] ?? 0),
        );
    }
}
