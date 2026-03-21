<?php

declare(strict_types=1);

namespace Nats\Protocol;

use Nats\NatsException;

final class Parser
{
    /**
     * Maximum control line size (matches Go client MAX_CONTROL_LINE_SIZE).
     */
    private const int MAX_CONTROL_LINE_SIZE = 4096;

    /**
     * Maximum allowed message payload size.
     * Set from server INFO max_payload. 0 = no limit (not yet connected).
     */
    private int $maxPayload = 0;

    private string $buffer = '';
    /** @var array{command: Command, subject: string, sid: string, replyTo: ?string, headerBytes: ?int, totalBytes: int}|null */
    private ?array $pendingMsg = null;

    /**
     * Feed raw data and return parsed operations.
     *
     * @return list<ServerOp>
     */
    public function parse(string $data): array
    {
        $this->buffer .= $data;
        $ops = [];

        while ($this->buffer !== '') {
            if ($this->pendingMsg !== null) {
                $op = $this->readPayload();
                if ($op === null) {
                    break;
                }
                $ops[] = $op;
                continue;
            }

            $crlfPos = strpos($this->buffer, "\r\n");
            if ($crlfPos === false) {
                // If buffer exceeds max control line size without a CRLF, protocol error
                if (strlen($this->buffer) > self::MAX_CONTROL_LINE_SIZE) {
                    $this->buffer = '';
                    throw new NatsException('Maximum control line size exceeded');
                }
                break;
            }

            if ($crlfPos > self::MAX_CONTROL_LINE_SIZE) {
                $this->buffer = '';
                throw new NatsException('Maximum control line size exceeded');
            }

            $line = substr($this->buffer, 0, $crlfPos);
            $this->buffer = substr($this->buffer, $crlfPos + 2);

            $op = $this->parseControlLine($line);
            if ($op !== null) {
                $ops[] = $op;
            }
        }

        return $ops;
    }

    public function reset(): void
    {
        $this->buffer = '';
        $this->pendingMsg = null;
    }

    /**
     * Set the maximum allowed payload size (from server INFO max_payload).
     */
    public function setMaxPayload(int $maxPayload): void
    {
        $this->maxPayload = $maxPayload;
    }

    private function parseControlLine(string $line): ?ServerOp
    {
        $spacePos = strpos($line, ' ');
        $command = $spacePos !== false ? substr($line, 0, $spacePos) : $line;
        $args = $spacePos !== false ? substr($line, $spacePos + 1) : '';

        return match ($command) {
            'INFO' => new ServerOp(
                command: Command::Info,
                payload: $args,
            ),
            'MSG' => $this->parseMsgArgs($args),
            'HMSG' => $this->parseHMsgArgs($args),
            'PING' => new ServerOp(command: Command::Ping),
            'PONG' => new ServerOp(command: Command::Pong),
            '+OK' => new ServerOp(command: Command::Ok),
            '-ERR' => new ServerOp(
                command: Command::Err,
                payload: trim($args, " '\""),
            ),
            default => throw new NatsException("Unknown protocol command: {$command}"),
        };
    }

    private function parseMsgArgs(string $args): ?ServerOp
    {
        $parts = preg_split('/\s+/', $args);
        if ($parts === false) {
            throw new NatsException("Invalid MSG args: {$args}");
        }
        $count = count($parts);

        // MSG <subject> <sid> [reply-to] <#bytes>
        if ($count === 3) {
            [$subject, $sid, $bytes] = $parts;
            $replyTo = null;
        } elseif ($count === 4) {
            [$subject, $sid, $replyTo, $bytes] = $parts;
        } else {
            throw new NatsException("Invalid MSG args: {$args}");
        }

        $totalBytes = (int) $bytes;
        if ($totalBytes < 0) {
            throw new NatsException("Bad or missing size in MSG: '{$args}'");
        }
        if ($this->maxPayload > 0 && $totalBytes > $this->maxPayload) {
            throw new NatsException("Message size {$totalBytes} exceeds max payload {$this->maxPayload}");
        }

        $this->pendingMsg = [
            'command' => Command::Msg,
            'subject' => $subject,
            'sid' => $sid,
            'replyTo' => $replyTo,
            'headerBytes' => null,
            'totalBytes' => $totalBytes,
        ];

        return $this->readPayload();
    }

    private function parseHMsgArgs(string $args): ?ServerOp
    {
        $parts = preg_split('/\s+/', $args);
        if ($parts === false) {
            throw new NatsException("Invalid HMSG args: {$args}");
        }
        $count = count($parts);

        // HMSG <subject> <sid> [reply-to] <#header-bytes> <#total-bytes>
        if ($count === 4) {
            [$subject, $sid, $headerBytes, $totalBytes] = $parts;
            $replyTo = null;
        } elseif ($count === 5) {
            [$subject, $sid, $replyTo, $headerBytes, $totalBytes] = $parts;
        } else {
            throw new NatsException("Invalid HMSG args: {$args}");
        }

        $hdrBytes = (int) $headerBytes;
        $totBytes = (int) $totalBytes;
        if ($totBytes < 0) {
            throw new NatsException("Bad or missing size in HMSG: '{$args}'");
        }
        if ($hdrBytes < 0 || $hdrBytes > $totBytes) {
            throw new NatsException("Bad or missing header size in HMSG: '{$args}'");
        }
        if ($this->maxPayload > 0 && $totBytes > $this->maxPayload) {
            throw new NatsException("Message size {$totBytes} exceeds max payload {$this->maxPayload}");
        }

        $this->pendingMsg = [
            'command' => Command::HMsg,
            'subject' => $subject,
            'sid' => $sid,
            'replyTo' => $replyTo,
            'headerBytes' => $hdrBytes,
            'totalBytes' => $totBytes,
        ];

        return $this->readPayload();
    }

    private function readPayload(): ?ServerOp
    {
        $msg = $this->pendingMsg;
        if ($msg === null) {
            return null;
        }
        $needed = $msg['totalBytes'] + 2; // payload + \r\n

        if (strlen($this->buffer) < $needed) {
            return null;
        }

        $payload = substr($this->buffer, 0, $msg['totalBytes']);
        $this->buffer = substr($this->buffer, $needed);
        $this->pendingMsg = null;

        return new ServerOp(
            command: $msg['command'],
            subject: $msg['subject'],
            sid: $msg['sid'],
            replyTo: $msg['replyTo'],
            headerBytes: $msg['headerBytes'],
            totalBytes: $msg['totalBytes'],
            payload: $payload,
        );
    }
}
