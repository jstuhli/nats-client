<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class StreamSource
{
    public function __construct(
        public string $name,
        public ?int $optStartSeq = null,
        public ?\DateTimeImmutable $optStartTime = null,
        public ?string $filterSubject = null,
        public ?string $domain = null,
        /** @var list<SubjectTransform> */
        public array $subjectTransforms = [],
        public ?ExternalStream $external = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = ['name' => $this->name];
        if ($this->optStartSeq !== null) { $data['opt_start_seq'] = $this->optStartSeq; }
        if ($this->optStartTime !== null) { $data['opt_start_time'] = $this->optStartTime->format('c'); }
        if ($this->filterSubject !== null) { $data['filter_subject'] = $this->filterSubject; }
        if ($this->external !== null) {
            $data['external'] = $this->external->toArray();
        } elseif ($this->domain !== null) {
            $data['external'] = ['api' => "\$JS.{$this->domain}.API"];
        }
        if ($this->subjectTransforms !== []) {
            $data['subject_transforms'] = array_map(
                fn(SubjectTransform $t) => $t->toArray(),
                $this->subjectTransforms,
            );
        }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $externalData = isset($data['external']) ? T::stringKeyArray($data['external']) : null;

        return new self(
            name: T::string($data['name'] ?? ''),
            optStartSeq: T::nullableInt($data['opt_start_seq'] ?? null),
            optStartTime: isset($data['opt_start_time']) ? new \DateTimeImmutable(T::string($data['opt_start_time'])) : null,
            filterSubject: T::nullableString($data['filter_subject'] ?? null),
            domain: $externalData !== null ? T::nullableString($externalData['api'] ?? null) : null,
            subjectTransforms: array_map(
                static fn(mixed $t): SubjectTransform => SubjectTransform::fromArray(T::stringKeyArray($t)),
                T::list($data['subject_transforms'] ?? []),
            ),
            external: $externalData !== null ? ExternalStream::fromArray($externalData) : null,
        );
    }
}
