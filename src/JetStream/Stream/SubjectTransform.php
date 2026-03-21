<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class SubjectTransform
{
    public function __construct(
        public string $source,
        public string $destination,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return ['src' => $this->source, 'dest' => $this->destination];
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            source: T::string($data['src'] ?? ''),
            destination: T::string($data['dest'] ?? ''),
        );
    }
}
