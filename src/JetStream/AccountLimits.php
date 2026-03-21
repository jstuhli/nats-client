<?php

declare(strict_types=1);

namespace Nats\JetStream;

use Nats\Internal\TypeCast as T;

readonly class AccountLimits
{
    public function __construct(
        public int $maxMemory = 0,
        public int $maxStorage = 0,
        public int $maxStreams = 0,
        public int $maxConsumers = 0,
        public int $maxAckPending = 0,
        public int $memoryMaxStreamBytes = 0,
        public int $storageMaxStreamBytes = 0,
        public bool $maxBytesRequired = false,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            maxMemory: T::int($data['max_memory'] ?? 0),
            maxStorage: T::int($data['max_storage'] ?? 0),
            maxStreams: T::int($data['max_streams'] ?? 0),
            maxConsumers: T::int($data['max_consumers'] ?? 0),
            maxAckPending: T::int($data['max_ack_pending'] ?? 0),
            memoryMaxStreamBytes: T::int($data['memory_max_stream_bytes'] ?? 0),
            storageMaxStreamBytes: T::int($data['storage_max_stream_bytes'] ?? 0),
            maxBytesRequired: T::bool($data['max_bytes_required'] ?? false),
        );
    }
}
