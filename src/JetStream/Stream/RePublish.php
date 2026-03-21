<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class RePublish
{
    public function __construct(
        public string $source,
        public string $destination,
        public bool $headersOnly = false,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return [
            'src' => $this->source,
            'dest' => $this->destination,
            'headers_only' => $this->headersOnly,
        ];
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            source: T::string($data['src'] ?? ''),
            destination: T::string($data['dest'] ?? ''),
            headersOnly: T::bool($data['headers_only'] ?? false),
        );
    }
}
