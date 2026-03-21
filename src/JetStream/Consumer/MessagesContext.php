<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\JetStream\Message\JetStreamMessage;
use Nats\TimeoutException;

/**
 * @implements \IteratorAggregate<int, JetStreamMessage>
 */
final class MessagesContext implements \IteratorAggregate
{
    /** @var list<JetStreamMessage> */
    private array $buffer = [];
    private bool $stopped = false;
    private bool $draining = false;

    /** @internal */
    public function __construct(
        private readonly Consumer $consumer,
        private readonly ConsumeOptions $options,
    ) {}

    public function next(float $timeout = 30.0): JetStreamMessage
    {
        if ($this->buffer !== []) {
            return array_shift($this->buffer);
        }

        // Fetch a small batch
        $batch = $this->consumer->fetch(
            $this->options->maxMessages,
            new FetchOptions(timeout: $timeout),
        );

        foreach ($batch->messages() as $msg) {
            $this->buffer[] = $msg;
        }

        if ($this->buffer !== []) {
            return array_shift($this->buffer);
        }

        throw new TimeoutException('No messages available');
    }

    public function stop(): void
    {
        $this->stopped = true;
    }

    public function drain(): void
    {
        $this->draining = true;
    }

    public function isClosed(): bool
    {
        return $this->stopped;
    }

    /** @return \Generator<JetStreamMessage> */
    public function getIterator(): \Generator
    {
        while (!$this->stopped) {
            try {
                $msg = $this->next($this->options->expires);
                yield $msg;
            } catch (TimeoutException) {
                if ($this->draining) {
                    $this->stopped = true;
                    return;
                }
                continue;
            }
        }
    }

    /** @internal */
    public function addMessage(JetStreamMessage $msg): void
    {
        $this->buffer[] = $msg;
    }
}
