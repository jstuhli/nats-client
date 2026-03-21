<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class ServiceStats
{
    /**
     * @param list<EndpointStats> $endpoints
     */
    public function __construct(
        public ServiceIdentity $identity,
        public string $type = 'io.nats.micro.v1.stats_response',
        public \DateTimeImmutable $started = new \DateTimeImmutable(),
        public array $endpoints = [],
        /** @var array<string, string> */
        public array $metadata = [],
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return array_merge($this->identity->toArray(), [
            'type' => $this->type,
            'started' => $this->started->format('c'),
            'endpoints' => array_map(fn(EndpointStats $e) => $e->toArray(), $this->endpoints),
            'metadata' => $this->metadata,
        ]);
    }
}
