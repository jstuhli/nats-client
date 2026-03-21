<?php

declare(strict_types=1);

namespace Nats\JetStream;

use Nats\Internal\TypeCast as T;

readonly class AccountInfo
{
    public function __construct(
        public int $memory = 0,
        public int $storage = 0,
        public int $streams = 0,
        public int $consumers = 0,
        public ?AccountLimits $limits = null,
        public ?string $domain = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            memory: T::int($data['memory'] ?? 0),
            storage: T::int($data['storage'] ?? 0),
            streams: T::int($data['streams'] ?? 0),
            consumers: T::int($data['consumers'] ?? 0),
            limits: isset($data['limits']) ? AccountLimits::fromArray(T::stringKeyArray($data['limits'])) : null,
            domain: T::nullableString($data['domain'] ?? null),
        );
    }
}
