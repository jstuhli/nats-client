<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\JetStream\Message\JetStreamMessage;

/**
 * @implements \IteratorAggregate<int, JetStreamMessage>
 */
final class MessageBatch implements \IteratorAggregate
{
    /** @var list<JetStreamMessage> */
    private array $messages = [];
    private ?\Throwable $error = null;
    private bool $complete = false;

    /** @internal */
    public function addMessage(JetStreamMessage $msg): void
    {
        $this->messages[] = $msg;
    }

    /** @internal */
    public function setError(\Throwable $error): void
    {
        $this->error = $error;
        $this->complete = true;
    }

    /** @internal */
    public function setComplete(): void
    {
        $this->complete = true;
    }

    public function isComplete(): bool
    {
        return $this->complete;
    }

    public function error(): ?\Throwable
    {
        return $this->error;
    }

    /** @return \Generator<JetStreamMessage> */
    public function messages(): \Generator
    {
        foreach ($this->messages as $msg) {
            yield $msg;
        }
    }

    public function getIterator(): \Generator
    {
        return $this->messages();
    }

    public function count(): int
    {
        return count($this->messages);
    }
}
