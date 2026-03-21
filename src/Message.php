<?php

declare(strict_types=1);

namespace Nats;

readonly class Message
{
    public function __construct(
        public string $subject,
        public string $data = '',
        public ?string $replyTo = null,
        public ?Headers $headers = null,
        private ?Connection $connection = null,
    ) {}

    public function size(): int
    {
        $size = strlen($this->data);
        if ($this->headers !== null) {
            $size += strlen($this->headers->toWireFormat());
        }
        return $size;
    }

    public function respond(string $data, ?Headers $headers = null): void
    {
        if ($this->replyTo === null) {
            throw new NatsException('No reply subject available');
        }
        if ($this->connection === null) {
            throw new NatsException('No connection available for respond');
        }

        $msg = new self(
            subject: $this->replyTo,
            data: $data,
            headers: $headers,
        );
        $this->connection->publishMessage($msg);
    }

    public function hasHeaders(): bool
    {
        return $this->headers !== null && $this->headers->count() > 0;
    }
}
