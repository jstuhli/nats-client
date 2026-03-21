<?php

declare(strict_types=1);

namespace Nats\JetStream;

readonly class JetStreamOptions
{
    public function __construct(
        public ?string $apiPrefix = null,
        public ?string $domain = null,
        public float $timeout = 5.0,
        public int $publishAsyncMaxPending = 4096,
        public ?\Closure $publishAsyncErrHandler = null,
    ) {}

    public function apiSubject(string $operation): string
    {
        $prefix = $this->apiPrefix ?? '$JS.API';
        if ($this->domain !== null) {
            $prefix = "\$JS.{$this->domain}.API";
        }
        return "{$prefix}.{$operation}";
    }
}
