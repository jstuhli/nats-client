<?php

declare(strict_types=1);

namespace Nats\Protocol;

final class Writer
{
    private const string CRLF = "\r\n";

    /**
     * @param array<string, mixed> $options
     */
    public function connect(array $options): string
    {
        return 'CONNECT ' . json_encode($options, JSON_UNESCAPED_SLASHES) . self::CRLF;
    }

    public function pub(string $subject, string $payload, ?string $replyTo = null): string
    {
        $size = strlen($payload);
        if ($replyTo !== null) {
            return "PUB {$subject} {$replyTo} {$size}" . self::CRLF . $payload . self::CRLF;
        }
        return "PUB {$subject} {$size}" . self::CRLF . $payload . self::CRLF;
    }

    public function hpub(string $subject, string $headers, string $payload, ?string $replyTo = null): string
    {
        $headerBytes = strlen($headers);
        $totalBytes = $headerBytes + strlen($payload);
        if ($replyTo !== null) {
            return "HPUB {$subject} {$replyTo} {$headerBytes} {$totalBytes}" . self::CRLF . $headers . $payload . self::CRLF;
        }
        return "HPUB {$subject} {$headerBytes} {$totalBytes}" . self::CRLF . $headers . $payload . self::CRLF;
    }

    public function sub(string $subject, string $sid, ?string $queue = null): string
    {
        if ($queue !== null) {
            return "SUB {$subject} {$queue} {$sid}" . self::CRLF;
        }
        return "SUB {$subject} {$sid}" . self::CRLF;
    }

    public function unsub(string $sid, ?int $maxMessages = null): string
    {
        if ($maxMessages !== null) {
            return "UNSUB {$sid} {$maxMessages}" . self::CRLF;
        }
        return "UNSUB {$sid}" . self::CRLF;
    }

    public function ping(): string
    {
        return 'PING' . self::CRLF;
    }

    public function pong(): string
    {
        return 'PONG' . self::CRLF;
    }
}
