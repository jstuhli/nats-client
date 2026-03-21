<?php

declare(strict_types=1);

namespace Nats\Transport;

interface TransportInterface
{
    /**
     * @param array<string, mixed>|null $tlsContext
     */
    public function connect(string $address, float $timeout, ?array $tlsContext = null): void;

    public function read(int $length): string;

    public function readLine(): string;

    public function write(string $data): int;

    public function close(): void;

    public function isConnected(): bool;

    public function setBlocking(bool $blocking): void;

    public function setTimeout(float $seconds): void;

    /**
     * Wait for data to be available for reading.
     *
     * @return bool True if data is available, false on timeout.
     */
    public function waitForData(float $timeout): bool;

    /**
     * Upgrade the connection to TLS.
     */
    /**
     * @param array<string, mixed> $tlsContext
     */
    public function upgradeTls(array $tlsContext): void;

    /**
     * @return resource|null The underlying stream resource.
     */
    public function getStreamResource(): mixed;
}
