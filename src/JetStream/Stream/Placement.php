<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class Placement
{
    public function __construct(
        public ?string $cluster = null,
        /** @var list<string> */
        public array $tags = [],
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];
        if ($this->cluster !== null) { $data['cluster'] = $this->cluster; }
        if ($this->tags !== []) { $data['tags'] = $this->tags; }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            cluster: T::nullableString($data['cluster'] ?? null),
            tags: array_map(static fn(mixed $v): string => T::string($v), T::list($data['tags'] ?? [])),
        );
    }
}
