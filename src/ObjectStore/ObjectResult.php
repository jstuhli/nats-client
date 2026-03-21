<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

final class ObjectResult
{
    private string $buffer = '';
    private bool $closed = false;

    public function __construct(
        private readonly ObjectInfo $info,
        string $data,
    ) {
        $this->buffer = $data;
    }

    public function read(int $length): string
    {
        if ($this->closed || $this->buffer === '') {
            return '';
        }
        $chunk = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);
        return $chunk;
    }

    public function readAll(): string
    {
        $data = $this->buffer;
        $this->buffer = '';
        return $data;
    }

    public function info(): ObjectInfo
    {
        return $this->info;
    }

    public function close(): void
    {
        $this->closed = true;
        $this->buffer = '';
    }

    /**
     * Write contents to a stream resource.
     *
     * @param resource $resource
     */
    public function writeTo(mixed $resource): int
    {
        $data = $this->readAll();
        $written = fwrite($resource, $data);
        return $written !== false ? $written : 0;
    }
}
