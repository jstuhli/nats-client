<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Enum\DeliveryPolicy;
use Nats\Enum\ReplayPolicy;

readonly class OrderedConsumerConfig
{
    public function __construct(
        public ?string $filterSubject = null,
        /** @var list<string> */
        public array $filterSubjects = [],
        public DeliveryPolicy $deliverPolicy = DeliveryPolicy::All,
        public ?int $optStartSeq = null,
        public ?\DateTimeImmutable $optStartTime = null,
        public ReplayPolicy $replayPolicy = ReplayPolicy::Instant,
        public bool $headersOnly = false,
        public ?float $maxResetAttempts = null,
    ) {}
}
