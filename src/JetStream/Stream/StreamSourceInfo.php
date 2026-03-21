<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamSourceInfo
{
    public function __construct(
        public string $name,
        public int $lag = 0,
        public float $active = 0,
        public ?string $filterSubject = null,
        /** @var list<SubjectTransform> */
        public array $subjectTransforms = [],
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: T::string($data['name'] ?? ''),
            lag: T::int($data['lag'] ?? 0),
            active: isset($data['active']) ? T::int($data['active']) / 1_000_000_000 : 0,
            filterSubject: T::nullableString($data['filter_subject'] ?? null),
            subjectTransforms: array_map(
                static fn(mixed $t): SubjectTransform => SubjectTransform::fromArray(T::stringKeyArray($t)),
                T::list($data['subject_transforms'] ?? []),
            ),
        );
    }
}
