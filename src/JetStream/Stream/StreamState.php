<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamState
{
    public function __construct(
        public int $messages,
        public int $bytes,
        public int $firstSeq,
        public ?\DateTimeImmutable $firstTs,
        public int $lastSeq,
        public ?\DateTimeImmutable $lastTs,
        public int $numSubjects = 0,
        public int $numDeleted = 0,
        public int $consumers = 0,
        /** @var array<string, int> Map of subject => message count */
        public array $subjects = [],
        /** @var list<int> List of deleted sequence numbers */
        public array $deleted = [],
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            messages: T::int($data['messages'] ?? 0),
            bytes: T::int($data['bytes'] ?? 0),
            firstSeq: T::int($data['first_seq'] ?? 0),
            firstTs: isset($data['first_ts']) ? new \DateTimeImmutable(T::string($data['first_ts'])) : null,
            lastSeq: T::int($data['last_seq'] ?? 0),
            lastTs: isset($data['last_ts']) ? new \DateTimeImmutable(T::string($data['last_ts'])) : null,
            numSubjects: T::int($data['num_subjects'] ?? 0),
            numDeleted: T::int($data['num_deleted'] ?? 0),
            consumers: T::int($data['consumer_count'] ?? 0),
            subjects: array_map(static fn(mixed $v): int => T::int($v), T::stringKeyArray($data['subjects'] ?? [])),
            deleted: array_map(static fn(mixed $v): int => T::int($v), T::list($data['deleted'] ?? [])),
        );
    }
}
