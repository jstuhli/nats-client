<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\JetStream\Stream\StreamInfo;

readonly class ObjectStoreStatus
{
    public function __construct(
        public string $bucket,
        public int $size,
        public int $objects,
        public ObjectStoreConfig $config,
        public string $backingStore,
        /** @var array<string, string> */
        public array $metadata = [],
        public bool $sealed = false,
        public bool $isCompressed = false,
        public ?StreamInfo $streamInfo = null,
    ) {}
}
