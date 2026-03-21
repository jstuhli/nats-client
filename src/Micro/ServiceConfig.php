<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class ServiceConfig
{
    public function __construct(
        public string $name,
        public string $version,
        public ?string $description = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?EndpointConfig $endpoint = null,
        public ?string $queueGroup = null,
        public bool $queueGroupDisabled = false,
        public ?\Closure $statsHandler = null,
        public ?\Closure $doneHandler = null,
        public ?\Closure $errorHandler = null,
    ) {}
}
