<?php

declare(strict_types=1);

namespace Nats\Transport;

use Nats\NatsException;

final class TcpTransport implements TransportInterface
{
    /** @var resource|null */
    private mixed $stream = null;

    /**
     * @param array<string, mixed>|null $tlsContext
     */
    public function connect(string $address, float $timeout, ?array $tlsContext = null): void
    {
        $contextOptions = [];
        if ($tlsContext !== null) {
            $contextOptions['ssl'] = $tlsContext;
        }

        $context = stream_context_create($contextOptions);
        $errno = 0;
        $errstr = '';

        $stream = @stream_socket_client(
            $address,
            $errno,
            $errstr,
            $timeout,
            STREAM_CLIENT_CONNECT,
            $context,
        );

        if ($stream === false) {
            throw new NatsException("Failed to connect to {$address}: [{$errno}] {$errstr}");
        }

        $this->stream = $stream;
        stream_set_read_buffer($this->stream, 0);
    }

    public function read(int $length): string
    {
        $stream = $this->getStream();
        $data = @fread($stream, max(1, $length));
        if ($data === false) {
            throw new NatsException('Failed to read from stream');
        }
        return $data;
    }

    public function readLine(): string
    {
        $stream = $this->getStream();
        $line = @fgets($stream);
        if ($line === false) {
            if (feof($stream)) {
                throw new NatsException('Connection closed by server');
            }
            throw new NatsException('Failed to read line from stream');
        }
        return $line;
    }

    public function write(string $data): int
    {
        $stream = $this->getStream();
        $totalWritten = 0;
        $length = strlen($data);

        while ($totalWritten < $length) {
            $written = @fwrite($stream, substr($data, $totalWritten));
            if ($written === false || $written === 0) {
                throw new NatsException('Failed to write to stream');
            }
            $totalWritten += $written;
        }

        return $totalWritten;
    }

    public function close(): void
    {
        if ($this->stream !== null) {
            @fclose($this->stream);
            $this->stream = null;
        }
    }

    public function isConnected(): bool
    {
        return $this->stream !== null && !feof($this->stream);
    }

    public function setBlocking(bool $blocking): void
    {
        stream_set_blocking($this->getStream(), $blocking);
    }

    public function setTimeout(float $seconds): void
    {
        $sec = (int) $seconds;
        $usec = (int) (($seconds - $sec) * 1_000_000);
        stream_set_timeout($this->getStream(), $sec, $usec);
    }

    public function waitForData(float $timeout): bool
    {
        $stream = $this->getStream();
        $read = [$stream];
        $write = null;
        $except = null;
        $sec = (int) $timeout;
        $usec = (int) (($timeout - $sec) * 1_000_000);

        $result = @stream_select($read, $write, $except, $sec, $usec);
        return $result !== false && $result > 0;
    }

    /**
     * @param array<string, mixed> $tlsContext
     */
    public function upgradeTls(array $tlsContext): void
    {
        $stream = $this->getStream();

        foreach ($tlsContext as $key => $value) {
            stream_context_set_option($stream, 'ssl', (string) $key, $value);
        }

        $result = @stream_socket_enable_crypto(
            $stream,
            true,
            STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT | STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT,
        );

        if ($result !== true) {
            throw new NatsException('Failed to upgrade connection to TLS');
        }
    }

    /**
     * @return resource|null
     */
    public function getStreamResource(): mixed
    {
        return $this->stream;
    }

    /**
     * @return resource
     */
    private function getStream(): mixed
    {
        if ($this->stream === null) {
            throw new NatsException('Not connected');
        }
        return $this->stream;
    }
}
