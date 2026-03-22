<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Enum\DiscardPolicy;
use Nats\Enum\RetentionPolicy;
use Nats\Enum\StorageType;
use Nats\Enum\StoreCompression;
use Nats\Internal\TypeCast as T;

readonly class StreamConfig
{
    public function __construct(
        public string $name,
        /** @var list<string> */
        public array $subjects = [],
        public ?string $description = null,
        public RetentionPolicy $retention = RetentionPolicy::Limits,
        public ?int $maxAge = null,
        public ?int $maxBytes = null,
        public ?int $maxMsgSize = null,
        public ?int $maxMessages = null,
        public ?int $maxMessagesPerSubject = null,
        public ?int $maxConsumers = null,
        public StorageType $storage = StorageType::File,
        public DiscardPolicy $discard = DiscardPolicy::Old,
        public int $replicas = 1,
        public bool $noAck = false,
        public bool $denyDelete = false,
        public bool $denyPurge = false,
        public bool $allowRollup = false,
        public StoreCompression $compression = StoreCompression::None,
        public ?int $firstSequence = null,
        public ?StreamSource $mirror = null,
        /** @var list<StreamSource> */
        public array $sources = [],
        public ?RePublish $rePublish = null,
        public ?Placement $placement = null,
        /** @var array<string, string> */
        public array $metadata = [],
        public bool $sealed = false,
        public bool $allowDirect = false,
        public bool $mirrorDirect = false,
        public bool $discardNewPerSubject = false,
        public ?int $duplicateWindow = null,
        public ?SubjectTransform $subjectTransform = null,
        public ?StreamConsumerLimits $consumerLimits = null,
        public bool $allowMsgTtl = false,
        public ?int $subjectDeleteMarkerTtl = null,
    ) {}

    // --- Withers ---

    /** @param list<string> $subjects */
    public function withSubjects(array $subjects): self
    {
        return clone($this, ['subjects' => $subjects]);
    }

    public function withDescription(?string $description): self
    {
        return clone($this, ['description' => $description]);
    }

    public function withRetention(RetentionPolicy $retention): self
    {
        return clone($this, ['retention' => $retention]);
    }

    public function withMaxAge(?int $maxAge): self
    {
        return clone($this, ['maxAge' => $maxAge]);
    }

    public function withMaxBytes(?int $maxBytes): self
    {
        return clone($this, ['maxBytes' => $maxBytes]);
    }

    public function withMaxMsgSize(?int $maxMsgSize): self
    {
        return clone($this, ['maxMsgSize' => $maxMsgSize]);
    }

    public function withMaxMessages(?int $maxMessages): self
    {
        return clone($this, ['maxMessages' => $maxMessages]);
    }

    public function withMaxMessagesPerSubject(?int $maxMessagesPerSubject): self
    {
        return clone($this, ['maxMessagesPerSubject' => $maxMessagesPerSubject]);
    }

    public function withMaxConsumers(?int $maxConsumers): self
    {
        return clone($this, ['maxConsumers' => $maxConsumers]);
    }

    public function withStorage(StorageType $storage): self
    {
        return clone($this, ['storage' => $storage]);
    }

    public function withDiscard(DiscardPolicy $discard): self
    {
        return clone($this, ['discard' => $discard]);
    }

    public function withReplicas(int $replicas): self
    {
        return clone($this, ['replicas' => $replicas]);
    }

    public function withNoAck(bool $noAck = true): self
    {
        return clone($this, ['noAck' => $noAck]);
    }

    public function withDenyDelete(bool $denyDelete = true): self
    {
        return clone($this, ['denyDelete' => $denyDelete]);
    }

    public function withDenyPurge(bool $denyPurge = true): self
    {
        return clone($this, ['denyPurge' => $denyPurge]);
    }

    public function withAllowRollup(bool $allowRollup = true): self
    {
        return clone($this, ['allowRollup' => $allowRollup]);
    }

    public function withCompression(StoreCompression $compression): self
    {
        return clone($this, ['compression' => $compression]);
    }

    public function withFirstSequence(?int $firstSequence): self
    {
        return clone($this, ['firstSequence' => $firstSequence]);
    }

    public function withMirror(?StreamSource $mirror): self
    {
        return clone($this, ['mirror' => $mirror]);
    }

    /** @param list<StreamSource> $sources */
    public function withSources(array $sources): self
    {
        return clone($this, ['sources' => $sources]);
    }

    public function withRePublish(?RePublish $rePublish): self
    {
        return clone($this, ['rePublish' => $rePublish]);
    }

    public function withPlacement(?Placement $placement): self
    {
        return clone($this, ['placement' => $placement]);
    }

    /** @param array<string, string> $metadata */
    public function withMetadata(array $metadata): self
    {
        return clone($this, ['metadata' => $metadata]);
    }

    public function withSealed(bool $sealed = true): self
    {
        return clone($this, ['sealed' => $sealed]);
    }

    public function withAllowDirect(bool $allowDirect = true): self
    {
        return clone($this, ['allowDirect' => $allowDirect]);
    }

    public function withMirrorDirect(bool $mirrorDirect = true): self
    {
        return clone($this, ['mirrorDirect' => $mirrorDirect]);
    }

    public function withDiscardNewPerSubject(bool $discardNewPerSubject = true): self
    {
        return clone($this, ['discardNewPerSubject' => $discardNewPerSubject]);
    }

    public function withDuplicateWindow(?int $duplicateWindow): self
    {
        return clone($this, ['duplicateWindow' => $duplicateWindow]);
    }

    public function withSubjectTransform(?SubjectTransform $subjectTransform): self
    {
        return clone($this, ['subjectTransform' => $subjectTransform]);
    }

    public function withConsumerLimits(?StreamConsumerLimits $consumerLimits): self
    {
        return clone($this, ['consumerLimits' => $consumerLimits]);
    }

    public function withAllowMsgTtl(bool $allowMsgTtl = true): self
    {
        return clone($this, ['allowMsgTtl' => $allowMsgTtl]);
    }

    public function withSubjectDeleteMarkerTtl(?int $subjectDeleteMarkerTtl): self
    {
        return clone($this, ['subjectDeleteMarkerTtl' => $subjectDeleteMarkerTtl]);
    }

    // --- Serialization ---

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = ['name' => $this->name];

        if ($this->subjects !== []) { $data['subjects'] = $this->subjects; }
        if ($this->description !== null) { $data['description'] = $this->description; }
        $data['retention'] = $this->retention->value;
        if ($this->maxAge !== null) { $data['max_age'] = $this->maxAge * 1_000_000_000; }
        if ($this->maxBytes !== null) { $data['max_bytes'] = $this->maxBytes; }
        if ($this->maxMsgSize !== null) { $data['max_msg_size'] = $this->maxMsgSize; }
        if ($this->maxMessages !== null) { $data['max_msgs'] = $this->maxMessages; }
        if ($this->maxMessagesPerSubject !== null) { $data['max_msgs_per_subject'] = $this->maxMessagesPerSubject; }
        if ($this->maxConsumers !== null) { $data['max_consumers'] = $this->maxConsumers; }
        $data['storage'] = $this->storage->value;
        $data['discard'] = $this->discard->value;
        $data['num_replicas'] = $this->replicas;
        if ($this->noAck) { $data['no_ack'] = true; }
        if ($this->denyDelete) { $data['deny_delete'] = true; }
        if ($this->denyPurge) { $data['deny_purge'] = true; }
        if ($this->allowRollup) { $data['allow_rollup_hdrs'] = true; }
        if ($this->compression !== StoreCompression::None) { $data['compression'] = $this->compression->value; }
        if ($this->firstSequence !== null) { $data['first_seq'] = $this->firstSequence; }
        if ($this->mirror !== null) { $data['mirror'] = $this->mirror->toArray(); }
        if ($this->sources !== []) { $data['sources'] = array_map(fn(StreamSource $s) => $s->toArray(), $this->sources); }
        if ($this->rePublish !== null) { $data['republish'] = $this->rePublish->toArray(); }
        if ($this->placement !== null) { $data['placement'] = $this->placement->toArray(); }
        if ($this->metadata !== []) { $data['metadata'] = $this->metadata; }
        if ($this->sealed) { $data['sealed'] = true; }
        if ($this->allowDirect) { $data['allow_direct'] = true; }
        if ($this->mirrorDirect) { $data['mirror_direct'] = true; }
        if ($this->discardNewPerSubject) { $data['discard_new_per_subject'] = true; }
        if ($this->duplicateWindow !== null) { $data['duplicate_window'] = $this->duplicateWindow * 1_000_000_000; }
        if ($this->subjectTransform !== null) { $data['subject_transform'] = $this->subjectTransform->toArray(); }
        if ($this->consumerLimits !== null) { $data['consumer_limits'] = $this->consumerLimits->toArray(); }
        if ($this->allowMsgTtl) { $data['allow_msg_ttl'] = true; }
        if ($this->subjectDeleteMarkerTtl !== null) { $data['subject_delete_marker_ttl'] = $this->subjectDeleteMarkerTtl * 1_000_000_000; }

        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            subjects: array_map(static fn(mixed $v): string => T::string($v), T::list($data['subjects'] ?? [])),
            description: T::nullableString($data['description'] ?? null),
            retention: RetentionPolicy::from(T::string($data['retention'] ?? 'limits')),
            maxAge: isset($data['max_age']) ? (int) (T::int($data['max_age']) / 1_000_000_000) : null,
            maxBytes: T::nullableInt($data['max_bytes'] ?? null),
            maxMsgSize: T::nullableInt($data['max_msg_size'] ?? null),
            maxMessages: T::nullableInt($data['max_msgs'] ?? null),
            maxMessagesPerSubject: T::nullableInt($data['max_msgs_per_subject'] ?? null),
            maxConsumers: T::nullableInt($data['max_consumers'] ?? null),
            storage: StorageType::from(T::string($data['storage'] ?? 'file')),
            discard: DiscardPolicy::from(T::string($data['discard'] ?? 'old')),
            replicas: T::int($data['num_replicas'] ?? 1),
            noAck: T::bool($data['no_ack'] ?? false),
            denyDelete: T::bool($data['deny_delete'] ?? false),
            denyPurge: T::bool($data['deny_purge'] ?? false),
            allowRollup: T::bool($data['allow_rollup_hdrs'] ?? false),
            compression: StoreCompression::from(T::string($data['compression'] ?? 'none')),
            firstSequence: T::nullableInt($data['first_seq'] ?? null),
            mirror: isset($data['mirror']) ? StreamSource::fromArray(T::stringKeyArray($data['mirror'])) : null,
            sources: array_map(
                static fn(mixed $s): StreamSource => StreamSource::fromArray(T::stringKeyArray($s)),
                T::list($data['sources'] ?? []),
            ),
            rePublish: isset($data['republish']) ? RePublish::fromArray(T::stringKeyArray($data['republish'])) : null,
            placement: isset($data['placement']) ? Placement::fromArray(T::stringKeyArray($data['placement'])) : null,
            metadata: T::stringMap($data['metadata'] ?? []),
            sealed: T::bool($data['sealed'] ?? false),
            allowDirect: T::bool($data['allow_direct'] ?? false),
            mirrorDirect: T::bool($data['mirror_direct'] ?? false),
            discardNewPerSubject: T::bool($data['discard_new_per_subject'] ?? false),
            duplicateWindow: isset($data['duplicate_window']) ? (int) (T::int($data['duplicate_window']) / 1_000_000_000) : null,
            subjectTransform: isset($data['subject_transform']) ? SubjectTransform::fromArray(T::stringKeyArray($data['subject_transform'])) : null,
            consumerLimits: isset($data['consumer_limits']) ? StreamConsumerLimits::fromArray(T::stringKeyArray($data['consumer_limits'])) : null,
            allowMsgTtl: T::bool($data['allow_msg_ttl'] ?? false),
            subjectDeleteMarkerTtl: isset($data['subject_delete_marker_ttl']) ? (int) (T::int($data['subject_delete_marker_ttl']) / 1_000_000_000) : null,
        );
    }
}
