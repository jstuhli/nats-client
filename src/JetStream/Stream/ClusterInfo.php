<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class ClusterInfo
{
    public function __construct(
        public string $name = '',
        public string $leader = '',
        /** @var list<PeerInfo> */
        public array $replicas = [],
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            leader: T::string($data['leader'] ?? ''),
            replicas: array_map(
                static fn(mixed $r): PeerInfo => PeerInfo::fromArray(T::stringKeyArray($r)),
                T::list($data['replicas'] ?? []),
            ),
        );
    }
}
