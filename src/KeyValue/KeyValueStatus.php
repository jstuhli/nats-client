<?php

declare(strict_types=1);

namespace Nats\KeyValue;

use Nats\JetStream\Stream\StreamInfo;

readonly class KeyValueStatus
{
    public function __construct(
        public string $bucket,
        public int $values,
        public int $history,
        public ?float $ttl,
        public int $bytes,
        public string $backingStore,
        public bool $isCompressed,
        public KeyValueConfig $config,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?StreamInfo $streamInfo = null,
        public ?float $limitMarkerTtl = null,
    ) {}
}
