<?php

declare(strict_types=1);

namespace Nats\Micro;

readonly class EndpointStats
{
    public function __construct(
        public string $name,
        public string $subject,
        public int $numRequests = 0,
        public int $numErrors = 0,
        public float $processingTime = 0,
        public float $averageProcessingTime = 0,
        public ?string $lastError = null,
        public mixed $data = null,
    ) {}

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $result = [
            'name' => $this->name,
            'subject' => $this->subject,
            'num_requests' => $this->numRequests,
            'num_errors' => $this->numErrors,
            'processing_time' => (int) ($this->processingTime * 1_000_000_000),
            'average_processing_time' => (int) ($this->averageProcessingTime * 1_000_000_000),
        ];
        if ($this->lastError !== null) {
            $result['last_error'] = $this->lastError;
        }
        if ($this->data !== null) {
            $result['data'] = $this->data;
        }
        return $result;
    }
}
