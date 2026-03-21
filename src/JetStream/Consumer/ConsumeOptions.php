<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

readonly class ConsumeOptions
{
    public function __construct(
        public int $maxMessages = 100,
        public int $maxBytes = 0,
        public float $expires = 30.0,
        public float $heartbeat = 5.0,
        public int $thresholdMessages = 0,
        public int $thresholdBytes = 0,
        public ?int $stopAfter = null,
        public ?\Closure $errorHandler = null,
    ) {}
}
