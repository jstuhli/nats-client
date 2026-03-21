<?php

declare(strict_types=1);

namespace Nats;

final class Subscription
{
    /** @var list<Message> Buffer for sync subscriptions */
    private array $messageBuffer = [];
    private int $delivered = 0;
    private int $dropped = 0;
    private int $maxMessages = 0;
    private bool $closed = false;
    private bool $draining = false;
    private int $pendingMsgLimit = 500_000;       // Go default: DefaultSubPendingMsgsLimit
    private int $pendingBytesLimit = 67_108_864; // 64MB — Go default: DefaultSubPendingBytesLimit
    private int $pendingBytes = 0;
    private int $maxPendingMsgs = 0;
    private int $maxPendingBytes = 0;
    private ?\Closure $closedHandler = null;

    /** @internal */
    public function __construct(
        private readonly Connection $connection,
        private readonly string $sid,
        private readonly string $subject,
        private readonly ?string $queue = null,
        private readonly ?\Closure $handler = null,
    ) {}

    public function nextMessage(float $timeout = 5.0): Message
    {
        if ($this->handler !== null) {
            throw new NatsException('Cannot call nextMessage on async subscription');
        }

        if ($this->closed) {
            throw new NatsException('Subscription is closed');
        }

        // Check buffer first
        $msg = array_shift($this->messageBuffer);
        if ($msg !== null) {
            $this->pendingBytes -= strlen($msg->data);
            return $msg;
        }

        // Poll for messages
        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            $remaining = $deadline - microtime(true);
            $this->connection->process(min(0.01, max(0.001, $remaining)));

            if ($this->messageBuffer !== []) {
                return $this->dequeueMessage();
            }
        }

        throw new TimeoutException('Subscription nextMessage timed out');
    }

    public function unsubscribe(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->connection->unsubscribeSid($this->sid);

        if ($this->closedHandler !== null) {
            ($this->closedHandler)();
        }
    }

    public function autoUnsubscribe(int $maxMessages): void
    {
        $this->maxMessages = $maxMessages;
        $this->connection->unsubscribeSid($this->sid, $maxMessages);
    }

    public function drain(): void
    {
        $this->draining = true;
        $this->connection->unsubscribeSid($this->sid);

        if ($this->closedHandler !== null) {
            ($this->closedHandler)();
        }
    }

    public function isValid(): bool
    {
        return !$this->closed;
    }

    public function isDraining(): bool
    {
        return $this->draining;
    }

    /**
     * @return array{messages: int, bytes: int}
     */
    public function pending(): array
    {
        return [
            'messages' => count($this->messageBuffer),
            'bytes' => $this->pendingBytes,
        ];
    }

    public function setPendingLimits(int $msgLimit, int $bytesLimit): void
    {
        $this->pendingMsgLimit = $msgLimit;
        $this->pendingBytesLimit = $bytesLimit;
    }

    public function delivered(): int
    {
        return $this->delivered;
    }

    public function dropped(): int
    {
        return $this->dropped;
    }

    public function subject(): string
    {
        return $this->subject;
    }

    public function queue(): ?string
    {
        return $this->queue;
    }

    public function sid(): string
    {
        return $this->sid;
    }

    /**
     * Returns the subscription type: 'sync' or 'async'.
     * Matches Go client's Subscription.Type() method.
     */
    public function type(): string
    {
        return $this->handler === null ? 'sync' : 'async';
    }

    /**
     * Returns the high-water mark for pending messages and bytes.
     *
     * @return array{messages: int, bytes: int}
     */
    public function maxPending(): array
    {
        return [
            'messages' => $this->maxPendingMsgs,
            'bytes' => $this->maxPendingBytes,
        ];
    }

    /**
     * Resets the high-water mark tracking for pending messages and bytes.
     */
    public function clearMaxPending(): void
    {
        $this->maxPendingMsgs = 0;
        $this->maxPendingBytes = 0;
    }

    /**
     * Returns the number of messages currently queued in the buffer.
     */
    public function queuedMsgs(): int
    {
        return count($this->messageBuffer);
    }

    /**
     * Returns the current pending limits.
     *
     * @return array{messages: int, bytes: int}
     */
    public function pendingLimits(): array
    {
        return [
            'messages' => $this->pendingMsgLimit,
            'bytes' => $this->pendingBytesLimit,
        ];
    }

    /**
     * Sets a handler to be called when the subscription is closed.
     */
    public function setClosedHandler(\Closure $handler): void
    {
        $this->closedHandler = $handler;
    }

    /** @internal Called by Connection when a message arrives */
    public function deliver(Message $msg): void
    {
        $this->delivered++;

        // Check auto-unsubscribe
        if ($this->maxMessages > 0 && $this->delivered >= $this->maxMessages) {
            $this->closed = true;
            $this->connection->removeSubscription($this->sid);
        }

        // Async handler
        if ($this->handler !== null) {
            ($this->handler)($msg);
            return;
        }

        // Sync buffer — slow consumer detection (matches Go client behavior)
        $msgSize = strlen($msg->data);
        if ((count($this->messageBuffer) >= $this->pendingMsgLimit)
            || ($this->pendingBytesLimit > 0 && $this->pendingBytes + $msgSize > $this->pendingBytesLimit)
        ) {
            $this->dropped++;
            $this->connection->reportSlowConsumer($this);
            return;
        }

        $this->messageBuffer[] = $msg;
        $this->pendingBytes += $msgSize;

        // Track high-water marks
        $currentMsgs = count($this->messageBuffer);
        if ($currentMsgs > $this->maxPendingMsgs) {
            $this->maxPendingMsgs = $currentMsgs;
        }
        if ($this->pendingBytes > $this->maxPendingBytes) {
            $this->maxPendingBytes = $this->pendingBytes;
        }
    }

    private function dequeueMessage(): Message
    {
        $msg = array_shift($this->messageBuffer);
        if ($msg === null) {
            throw new NatsException('No message in buffer');
        }
        $this->pendingBytes -= strlen($msg->data);
        return $msg;
    }
}
