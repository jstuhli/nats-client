<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

readonly class StreamPurgeOptions
{
    public function __construct(
        public ?string $filter = null,
        public ?int $sequence = null,
        public ?int $keep = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $data = [];
        if ($this->filter !== null) { $data['filter'] = $this->filter; }
        if ($this->sequence !== null) { $data['seq'] = $this->sequence; }
        if ($this->keep !== null) { $data['keep'] = $this->keep; }
        return $data;
    }
}
