<?php

declare(strict_types=1);

namespace Nats\Protocol;

readonly class ServerOp
{
    public function __construct(
        public Command $command,
        public ?string $subject = null,
        public ?string $sid = null,
        public ?string $replyTo = null,
        public ?int $headerBytes = null,
        public ?int $totalBytes = null,
        public ?string $payload = null,
        public ?string $rawArgs = null,
    ) {}
}
