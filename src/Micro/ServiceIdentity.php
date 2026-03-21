<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class ServiceIdentity
{
    public function __construct(
        public string $name,
        public string $id,
        public string $version,
        /** @var array<string, string> */
        public array $metadata = [],
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [
            'name' => $this->name,
            'id' => $this->id,
            'version' => $this->version,
        ];
        if ($this->metadata !== []) {
            $data['metadata'] = $this->metadata;
        }
        return $data;
    }
}
