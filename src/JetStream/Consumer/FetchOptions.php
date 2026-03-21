<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

readonly class FetchOptions
{
    public function __construct(
        public float $timeout = 30.0,
        public ?float $heartbeat = null,
        public ?int $minPending = null,
        public ?int $minAckPending = null,
    ) {}
}
