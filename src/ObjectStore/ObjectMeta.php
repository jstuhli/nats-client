<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Headers;
use Nats\Internal\TypeCast as T;

readonly class ObjectMeta
{
    public function __construct(
        public string $name,
        public ?string $description = null,
        public ?Headers $headers = null,
        public ?ObjectLink $link = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?int $chunkSize = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = ['name' => $this->name];
        if ($this->description !== null) {
            $data['description'] = $this->description;
        }
        if ($this->link !== null) {
            $data['link'] = $this->link->toArray();
        }
        if ($this->metadata !== []) {
            $data['metadata'] = $this->metadata;
        }
        if ($this->chunkSize !== null) {
            $data['options'] = ['max_chunk_size' => $this->chunkSize];
        }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $options = T::stringKeyArray($data['options'] ?? []);

        return new self(
            name: T::string($data['name'] ?? ''),
            description: T::nullableString($data['description'] ?? null),
            link: isset($data['link']) ? ObjectLink::fromArray(T::stringKeyArray($data['link'])) : null,
            metadata: T::stringMap($data['metadata'] ?? []),
            chunkSize: T::nullableInt($options['max_chunk_size'] ?? null),
        );
    }
}
