<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Enum\AckPolicy;
use Nats\Enum\DeliveryPolicy;
use Nats\Enum\ReplayPolicy;
use Nats\Internal\TypeCast as T;

readonly class ConsumerConfig
{
    public function __construct(
        public ?string $name = null,
        public ?string $durable = null,
        public ?string $description = null,
        public DeliveryPolicy $deliverPolicy = DeliveryPolicy::All,
        public ?int $optStartSeq = null,
        public ?\DateTimeImmutable $optStartTime = null,
        public AckPolicy $ackPolicy = AckPolicy::Explicit,
        public ?float $ackWait = null,
        public int $maxDeliver = -1,
        /** @var list<float> */
        public array $backOff = [],
        public int $replicas = 0,
        public int $maxWaiting = 512,
        public int $maxAckPending = 1000,
        public bool $headersOnly = false,
        /** @var array<string, string> */
        public array $metadata = [],
        public ?string $deliverSubject = null,
        public ?string $deliverGroup = null,
        public ?string $filterSubject = null,
        /** @var list<string> */
        public array $filterSubjects = [],
        public ?float $inactiveThreshold = null,
        public ?\DateTimeImmutable $pauseUntil = null,
        public ReplayPolicy $replayPolicy = ReplayPolicy::Instant,
        public ?int $rateLimit = null,
        public bool $flowControl = false,
        public ?float $idleHeartbeat = null,
        public int $maxBatch = 0,
        public int $maxBytes = 0,
        public bool $memoryStorage = false,
        public ?string $sampleFrequency = null,
        public ?float $maxRequestExpires = null,
        public ?int $maxRequestMaxBytes = null,
        public ?string $priorityPolicy = null,
        public ?float $pinnedTtl = null,
        /** @var list<string> */
        public array $priorityGroups = [],
    ) {}

    // --- Withers ---

    public function withName(?string $name): self
    {
        return clone($this, ['name' => $name]);
    }

    public function withDurable(?string $durable): self
    {
        return clone($this, ['durable' => $durable]);
    }

    public function withDescription(?string $description): self
    {
        return clone($this, ['description' => $description]);
    }

    public function withDeliverPolicy(DeliveryPolicy $deliverPolicy): self
    {
        return clone($this, ['deliverPolicy' => $deliverPolicy]);
    }

    public function withOptStartSeq(?int $optStartSeq): self
    {
        return clone($this, ['optStartSeq' => $optStartSeq]);
    }

    public function withOptStartTime(?\DateTimeImmutable $optStartTime): self
    {
        return clone($this, ['optStartTime' => $optStartTime]);
    }

    public function withAckPolicy(AckPolicy $ackPolicy): self
    {
        return clone($this, ['ackPolicy' => $ackPolicy]);
    }

    public function withAckWait(?float $ackWait): self
    {
        return clone($this, ['ackWait' => $ackWait]);
    }

    public function withMaxDeliver(int $maxDeliver): self
    {
        return clone($this, ['maxDeliver' => $maxDeliver]);
    }

    /** @param list<float> $backOff */
    public function withBackOff(array $backOff): self
    {
        return clone($this, ['backOff' => $backOff]);
    }

    public function withReplicas(int $replicas): self
    {
        return clone($this, ['replicas' => $replicas]);
    }

    public function withMaxWaiting(int $maxWaiting): self
    {
        return clone($this, ['maxWaiting' => $maxWaiting]);
    }

    public function withMaxAckPending(int $maxAckPending): self
    {
        return clone($this, ['maxAckPending' => $maxAckPending]);
    }

    public function withHeadersOnly(bool $headersOnly = true): self
    {
        return clone($this, ['headersOnly' => $headersOnly]);
    }

    /** @param array<string, string> $metadata */
    public function withMetadata(array $metadata): self
    {
        return clone($this, ['metadata' => $metadata]);
    }

    public function withDeliverSubject(?string $deliverSubject): self
    {
        return clone($this, ['deliverSubject' => $deliverSubject]);
    }

    public function withDeliverGroup(?string $deliverGroup): self
    {
        return clone($this, ['deliverGroup' => $deliverGroup]);
    }

    public function withFilterSubject(?string $filterSubject): self
    {
        return clone($this, ['filterSubject' => $filterSubject]);
    }

    /** @param list<string> $filterSubjects */
    public function withFilterSubjects(array $filterSubjects): self
    {
        return clone($this, ['filterSubjects' => $filterSubjects]);
    }

    public function withInactiveThreshold(?float $inactiveThreshold): self
    {
        return clone($this, ['inactiveThreshold' => $inactiveThreshold]);
    }

    public function withPauseUntil(?\DateTimeImmutable $pauseUntil): self
    {
        return clone($this, ['pauseUntil' => $pauseUntil]);
    }

    public function withReplayPolicy(ReplayPolicy $replayPolicy): self
    {
        return clone($this, ['replayPolicy' => $replayPolicy]);
    }

    public function withRateLimit(?int $rateLimit): self
    {
        return clone($this, ['rateLimit' => $rateLimit]);
    }

    public function withFlowControl(bool $flowControl = true): self
    {
        return clone($this, ['flowControl' => $flowControl]);
    }

    public function withIdleHeartbeat(?float $idleHeartbeat): self
    {
        return clone($this, ['idleHeartbeat' => $idleHeartbeat]);
    }

    public function withMaxBatch(int $maxBatch): self
    {
        return clone($this, ['maxBatch' => $maxBatch]);
    }

    public function withMaxBytes(int $maxBytes): self
    {
        return clone($this, ['maxBytes' => $maxBytes]);
    }

    public function withMemoryStorage(bool $memoryStorage = true): self
    {
        return clone($this, ['memoryStorage' => $memoryStorage]);
    }

    public function withSampleFrequency(?string $sampleFrequency): self
    {
        return clone($this, ['sampleFrequency' => $sampleFrequency]);
    }

    public function withMaxRequestExpires(?float $maxRequestExpires): self
    {
        return clone($this, ['maxRequestExpires' => $maxRequestExpires]);
    }

    public function withMaxRequestMaxBytes(?int $maxRequestMaxBytes): self
    {
        return clone($this, ['maxRequestMaxBytes' => $maxRequestMaxBytes]);
    }

    public function withPriorityPolicy(?string $priorityPolicy): self
    {
        return clone($this, ['priorityPolicy' => $priorityPolicy]);
    }

    public function withPinnedTtl(?float $pinnedTtl): self
    {
        return clone($this, ['pinnedTtl' => $pinnedTtl]);
    }

    /** @param list<string> $priorityGroups */
    public function withPriorityGroups(array $priorityGroups): self
    {
        return clone($this, ['priorityGroups' => $priorityGroups]);
    }

    // --- Serialization ---

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];

        if ($this->name !== null) { $data['name'] = $this->name; }
        if ($this->durable !== null) { $data['durable_name'] = $this->durable; }
        if ($this->description !== null) { $data['description'] = $this->description; }
        $data['deliver_policy'] = $this->deliverPolicy->value;
        if ($this->optStartSeq !== null) { $data['opt_start_seq'] = $this->optStartSeq; }
        if ($this->optStartTime !== null) { $data['opt_start_time'] = $this->optStartTime->format('c'); }
        $data['ack_policy'] = $this->ackPolicy->value;
        if ($this->ackWait !== null) { $data['ack_wait'] = (int) ($this->ackWait * 1_000_000_000); }
        if ($this->maxDeliver !== -1) { $data['max_deliver'] = $this->maxDeliver; }
        if ($this->backOff !== []) {
            $data['backoff'] = array_map(fn(float $s) => (int) ($s * 1_000_000_000), $this->backOff);
        }
        if ($this->replicas > 0) { $data['num_replicas'] = $this->replicas; }
        $data['max_waiting'] = $this->maxWaiting;
        $data['max_ack_pending'] = $this->maxAckPending;
        if ($this->headersOnly) { $data['headers_only'] = true; }
        if ($this->metadata !== []) { $data['metadata'] = $this->metadata; }
        if ($this->deliverSubject !== null) { $data['deliver_subject'] = $this->deliverSubject; }
        if ($this->deliverGroup !== null) { $data['deliver_group'] = $this->deliverGroup; }
        if ($this->filterSubject !== null) { $data['filter_subject'] = $this->filterSubject; }
        if ($this->filterSubjects !== []) { $data['filter_subjects'] = $this->filterSubjects; }
        if ($this->inactiveThreshold !== null) { $data['inactive_threshold'] = (int) ($this->inactiveThreshold * 1_000_000_000); }
        if ($this->pauseUntil !== null) { $data['pause_until'] = $this->pauseUntil->format('c'); }
        $data['replay_policy'] = $this->replayPolicy->value;
        if ($this->rateLimit !== null) { $data['rate_limit_bps'] = $this->rateLimit; }
        if ($this->flowControl) { $data['flow_control'] = true; }
        if ($this->idleHeartbeat !== null) { $data['idle_heartbeat'] = (int) ($this->idleHeartbeat * 1_000_000_000); }
        if ($this->maxBatch > 0) { $data['max_batch'] = $this->maxBatch; }
        if ($this->maxBytes > 0) { $data['max_bytes'] = $this->maxBytes; }
        if ($this->memoryStorage) { $data['mem_storage'] = true; }
        if ($this->sampleFrequency !== null) { $data['sample_freq'] = $this->sampleFrequency; }
        if ($this->maxRequestExpires !== null) { $data['max_expires'] = (int) ($this->maxRequestExpires * 1_000_000_000); }
        if ($this->maxRequestMaxBytes !== null) { $data['max_bytes'] = $this->maxRequestMaxBytes; }
        if ($this->priorityPolicy !== null) { $data['priority_policy'] = $this->priorityPolicy; }
        if ($this->pinnedTtl !== null) { $data['pinned_ttl'] = (int) ($this->pinnedTtl * 1_000_000_000); }
        if ($this->priorityGroups !== []) { $data['priority_groups'] = $this->priorityGroups; }

        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::nullableString($data['name'] ?? null),
            durable: T::nullableString($data['durable_name'] ?? null),
            description: T::nullableString($data['description'] ?? null),
            deliverPolicy: DeliveryPolicy::from(T::string($data['deliver_policy'] ?? 'all')),
            optStartSeq: T::nullableInt($data['opt_start_seq'] ?? null),
            optStartTime: isset($data['opt_start_time']) ? new \DateTimeImmutable(T::string($data['opt_start_time'])) : null,
            ackPolicy: AckPolicy::from(T::string($data['ack_policy'] ?? 'explicit')),
            ackWait: isset($data['ack_wait']) ? T::int($data['ack_wait']) / 1_000_000_000 : null,
            maxDeliver: T::int($data['max_deliver'] ?? -1),
            backOff: array_map(static fn(mixed $ns): float => T::int($ns) / 1_000_000_000, T::list($data['backoff'] ?? [])),
            replicas: T::int($data['num_replicas'] ?? 0),
            maxWaiting: T::int($data['max_waiting'] ?? 512),
            maxAckPending: T::int($data['max_ack_pending'] ?? 1000),
            headersOnly: T::bool($data['headers_only'] ?? false),
            metadata: T::stringMap($data['metadata'] ?? []),
            deliverSubject: T::nullableString($data['deliver_subject'] ?? null),
            deliverGroup: T::nullableString($data['deliver_group'] ?? null),
            filterSubject: T::nullableString($data['filter_subject'] ?? null),
            filterSubjects: array_map(static fn(mixed $v): string => T::string($v), T::list($data['filter_subjects'] ?? [])),
            inactiveThreshold: isset($data['inactive_threshold']) ? T::int($data['inactive_threshold']) / 1_000_000_000 : null,
            pauseUntil: isset($data['pause_until']) ? new \DateTimeImmutable(T::string($data['pause_until'])) : null,
            replayPolicy: ReplayPolicy::from(T::string($data['replay_policy'] ?? 'instant')),
            rateLimit: T::nullableInt($data['rate_limit_bps'] ?? null),
            flowControl: T::bool($data['flow_control'] ?? false),
            idleHeartbeat: isset($data['idle_heartbeat']) ? T::int($data['idle_heartbeat']) / 1_000_000_000 : null,
            maxBatch: T::int($data['max_batch'] ?? 0),
            maxBytes: T::int($data['max_bytes'] ?? 0),
            memoryStorage: T::bool($data['mem_storage'] ?? false),
            sampleFrequency: T::nullableString($data['sample_freq'] ?? null),
            maxRequestExpires: isset($data['max_expires']) ? T::int($data['max_expires']) / 1_000_000_000 : null,
            maxRequestMaxBytes: T::nullableInt($data['max_bytes'] ?? null),
            priorityPolicy: T::nullableString($data['priority_policy'] ?? null),
            pinnedTtl: isset($data['pinned_ttl']) ? T::int($data['pinned_ttl']) / 1_000_000_000 : null,
            priorityGroups: array_map(static fn(mixed $v): string => T::string($v), T::list($data['priority_groups'] ?? [])),
        );
    }
}
