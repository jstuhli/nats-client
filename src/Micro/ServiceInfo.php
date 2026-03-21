<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class ServiceInfo
{
    /**
     * @param list<array{name: string, subject: string, metadata?: array<string, string>}> $endpoints
     */
    public function __construct(
        public ServiceIdentity $identity,
        public string $type = 'io.nats.micro.v1.info_response',
        public ?string $description = null,
        public array $endpoints = [],
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = array_merge($this->identity->toArray(), [
            'type' => $this->type,
        ]);
        if ($this->description !== null) {
            $data['description'] = $this->description;
        }
        if ($this->endpoints !== []) {
            $data['endpoints'] = $this->endpoints;
        }
        return $data;
    }
}
