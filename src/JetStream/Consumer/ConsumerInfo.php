<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Internal\TypeCast as T;
use Nats\JetStream\Stream\ClusterInfo;

readonly class ConsumerInfo
{
    public function __construct(
        public string $stream,
        public string $name,
        public ConsumerConfig $config,
        public int $numPending = 0,
        public int $numAckPending = 0,
        public int $numRedelivered = 0,
        public int $numWaiting = 0,
        public ?\DateTimeImmutable $created = null,
        public bool $pushBound = false,
        public bool $paused = false,
        public ?float $pauseRemaining = null,
        public ?ClusterInfo $cluster = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            stream: T::string($data['stream_name'] ?? ''),
            name: T::string($data['name'] ?? ''),
            config: ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])),
            numPending: T::int($data['num_pending'] ?? 0),
            numAckPending: T::int($data['num_ack_pending'] ?? 0),
            numRedelivered: T::int($data['num_redelivered'] ?? 0),
            numWaiting: T::int($data['num_waiting'] ?? 0),
            created: isset($data['created']) ? new \DateTimeImmutable(T::string($data['created'])) : null,
            pushBound: T::bool($data['push_bound'] ?? false),
            paused: T::bool($data['paused'] ?? false),
            pauseRemaining: isset($data['pause_remaining']) ? T::int($data['pause_remaining']) / 1_000_000_000 : null,
            cluster: isset($data['cluster']) ? ClusterInfo::fromArray(T::stringKeyArray($data['cluster'])) : null,
        );
    }
}
