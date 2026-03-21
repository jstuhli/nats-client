<?php

declare(strict_types=1);

namespace Nats\JetStream\Publish;

use Nats\Message;
use Nats\TimeoutException;

final class PubAckFuture
{
    private ?PubAck $result = null;
    private ?\Throwable $error = null;
    private bool $complete = false;

    public function __construct(
        private readonly Message $originalMessage,
    ) {}

    public function ok(float $timeout = 5.0): PubAck
    {
        if ($this->result !== null) {
            return $this->result;
        }
        if ($this->error !== null) {
            throw $this->error;
        }

        // If running inside a Fiber, suspend and wait
        if (\Fiber::getCurrent() !== null) {
            $deadline = microtime(true) + $timeout;
            while (!$this->complete && microtime(true) < $deadline) {
                \Fiber::suspend();
            }
        }

        if ($this->result !== null) {
            return $this->result;
        }
        if ($this->error !== null) {
            throw $this->error;
        }

        throw new TimeoutException('PubAckFuture timed out');
    }

    public function error(): ?\Throwable
    {
        return $this->error;
    }

    public function message(): Message
    {
        return $this->originalMessage;
    }

    public function isComplete(): bool
    {
        return $this->complete;
    }

    /** @internal */
    public function resolve(PubAck $ack): void
    {
        $this->result = $ack;
        $this->complete = true;
    }

    /** @internal */
    public function reject(\Throwable $error): void
    {
        $this->error = $error;
        $this->complete = true;
    }
}
