<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamAlternate
{
    public function __construct(
        public string $name,
        public string $domain = '',
        public string $cluster = '',
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            domain: T::string($data['domain'] ?? ''),
            cluster: T::string($data['cluster'] ?? ''),
        );
    }
}
