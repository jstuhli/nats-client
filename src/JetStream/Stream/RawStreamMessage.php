<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Headers;
use Nats\Internal\TypeCast as T;

readonly class RawStreamMessage
{
    public function __construct(
        public string $subject,
        public int $sequence,
        public string $data,
        public ?\DateTimeImmutable $time = null,
        public ?Headers $headers = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $encodedData = T::string($data['data'] ?? '');
        $payload = $encodedData !== '' ? base64_decode($encodedData) : '';
        $headers = null;
        if (isset($data['hdrs'])) {
            $headerRaw = base64_decode(T::string($data['hdrs']));
            $headers = Headers::fromWireFormat($headerRaw);
        }

        return new self(
            subject: T::string($data['subject'] ?? ''),
            sequence: T::int($data['seq'] ?? 0),
            data: $payload,
            time: isset($data['time']) ? new \DateTimeImmutable(T::string($data['time'])) : null,
            headers: $headers,
        );
    }
}
