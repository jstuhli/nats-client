<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Headers;
use Nats\Internal\TypeCast as T;

readonly class ObjectInfo
{
    public function __construct(
        public string $name,
        public string $bucket,
        public string $nuid,
        public int $size,
        public int $chunks,
        public string $digest,
        public bool $deleted,
        public ?\DateTimeImmutable $mtime = null,
        public ?string $description = null,
        public ?Headers $headers = null,
        public ?ObjectLink $link = null,
        /** @var array<string, string> */
        public array $metadata = [],
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            bucket: T::string($data['bucket'] ?? ''),
            nuid: T::string($data['nuid'] ?? ''),
            size: T::int($data['size'] ?? 0),
            chunks: T::int($data['chunks'] ?? 0),
            digest: T::string($data['digest'] ?? ''),
            deleted: T::bool($data['deleted'] ?? false),
            mtime: isset($data['mtime']) ? new \DateTimeImmutable(T::string($data['mtime'])) : null,
            description: T::nullableString($data['description'] ?? null),
            link: isset($data['link']) ? ObjectLink::fromArray(T::stringKeyArray($data['link'])) : null,
            metadata: T::stringMap($data['metadata'] ?? []),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [
            'name' => $this->name,
            'bucket' => $this->bucket,
            'nuid' => $this->nuid,
            'size' => $this->size,
            'chunks' => $this->chunks,
        ];
        if ($this->digest !== '') { $data['digest'] = $this->digest; }
        if ($this->deleted) { $data['deleted'] = true; }
        if ($this->mtime !== null) { $data['mtime'] = $this->mtime->format('c'); }
        if ($this->description !== null) { $data['description'] = $this->description; }
        if ($this->link !== null) { $data['link'] = $this->link->toArray(); }
        if ($this->metadata !== []) { $data['metadata'] = $this->metadata; }
        return $data;
    }

    public function isLink(): bool
    {
        return $this->link !== null && ($this->link->bucket !== '' || $this->link->name !== '');
    }
}
