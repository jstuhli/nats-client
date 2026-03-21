<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamInfo
{
    public function __construct(
        public StreamConfig $config,
        public StreamState $state,
        public ?\DateTimeImmutable $created = null,
        public ?ClusterInfo $cluster = null,
        public ?StreamSourceInfo $mirror = null,
        /** @var list<StreamSourceInfo> */
        public array $sources = [],
        /** @var list<StreamAlternate> */
        public array $alternates = [],
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            config: StreamConfig::fromArray(T::stringKeyArray($data['config'] ?? [])),
            state: StreamState::fromArray(T::stringKeyArray($data['state'] ?? [])),
            created: isset($data['created']) ? new \DateTimeImmutable(T::string($data['created'])) : null,
            cluster: isset($data['cluster']) ? ClusterInfo::fromArray(T::stringKeyArray($data['cluster'])) : null,
            mirror: isset($data['mirror']) ? StreamSourceInfo::fromArray(T::stringKeyArray($data['mirror'])) : null,
            sources: array_map(
                static fn(mixed $s): StreamSourceInfo => StreamSourceInfo::fromArray(T::stringKeyArray($s)),
                T::list($data['sources'] ?? []),
            ),
            alternates: array_map(
                static fn(mixed $a): StreamAlternate => StreamAlternate::fromArray(T::stringKeyArray($a)),
                T::list($data['alternates'] ?? []),
            ),
        );
    }
}
