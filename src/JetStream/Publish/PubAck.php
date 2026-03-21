<?php

declare(strict_types=1);

namespace Nats\JetStream\Publish;

use Nats\Internal\TypeCast as T;

readonly class PubAck
{
    public function __construct(
        public string $stream,
        public int $sequence,
        public bool $duplicate = false,
        public ?string $domain = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            stream: T::string($data['stream'] ?? ''),
            sequence: T::int($data['seq'] ?? 0),
            duplicate: T::bool($data['duplicate'] ?? false),
            domain: T::nullableString($data['domain'] ?? null),
        );
    }
}
