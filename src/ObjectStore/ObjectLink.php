<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Internal\TypeCast as T;

readonly class ObjectLink
{
    public function __construct(
        public string $bucket = '',
        public string $name = '',
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];
        if ($this->bucket !== '') {
            $data['bucket'] = $this->bucket;
        }
        if ($this->name !== '') {
            $data['name'] = $this->name;
        }
        return $data;
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            bucket: T::string($data['bucket'] ?? ''),
            name: T::string($data['name'] ?? ''),
        );
    }
}
