<?php

declare(strict_types=1);

namespace Nats\Transport;

final class Buffer
{
    private string $data = '';
    private int $writePos = 0;

    public function __construct(
        private readonly int $maxSize = 1_048_576, // 1MB default
    ) {}

    public function append(string $data): void
    {
        $this->data .= $data;
        $this->writePos += strlen($data);
    }

    public function consume(int $length): string
    {
        if ($length > strlen($this->data)) {
            $length = strlen($this->data);
        }
        $chunk = substr($this->data, 0, $length);
        $this->data = substr($this->data, $length);
        $this->writePos = strlen($this->data);
        return $chunk;
    }

    public function peek(int $length): string
    {
        return substr($this->data, 0, $length);
    }

    public function length(): int
    {
        return strlen($this->data);
    }

    public function isEmpty(): bool
    {
        return $this->data === '';
    }

    public function isFull(): bool
    {
        return strlen($this->data) >= $this->maxSize;
    }

    public function flush(): string
    {
        $data = $this->data;
        $this->data = '';
        $this->writePos = 0;
        return $data;
    }

    public function clear(): void
    {
        $this->data = '';
        $this->writePos = 0;
    }

    public function maxSize(): int
    {
        return $this->maxSize;
    }
}
