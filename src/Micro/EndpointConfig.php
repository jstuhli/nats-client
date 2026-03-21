<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class EndpointConfig
{
    public function __construct(
        public ?string $subject = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?string $queueGroup = null,
        public bool $queueGroupDisabled = false,
        public ?int $pendingMsgLimit = null,
        public ?int $pendingBytesLimit = null,
    ) {}
}
