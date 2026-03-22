<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Enum\StorageType;
use Nats\JetStream\Stream\Placement;

readonly class ObjectStoreConfig
{
    public function __construct(
        public string $bucket,
        public ?string $description = null,
        public ?float $ttl = null,
        public ?int $maxBytes = null,
        public StorageType $storage = StorageType::File,
        public int $replicas = 1,
        public ?Placement $placement = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?int $maxChunkSize = null,
        public bool $compression = false,
    ) {
        if ($this->maxChunkSize !== null && $this->maxChunkSize <= 0) {
            throw new \InvalidArgumentException('maxChunkSize must be greater than 0');
        }
    }
}
