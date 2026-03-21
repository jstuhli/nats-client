<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;

readonly class ExternalStream
{
    public function __construct(
        public string $apiPrefix = '',
        public string $deliverPrefix = '',
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];
        if ($this->apiPrefix !== '') { $data['api'] = $this->apiPrefix; }
        if ($this->deliverPrefix !== '') { $data['deliver'] = $this->deliverPrefix; }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            apiPrefix: T::string($data['api'] ?? ''),
            deliverPrefix: T::string($data['deliver'] ?? ''),
        );
    }
}
