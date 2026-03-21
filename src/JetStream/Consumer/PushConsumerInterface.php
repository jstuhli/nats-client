<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

interface PushConsumerInterface
{
    /**
     * Returns information about the push consumer.
     */
    public function info(): ConsumerInfo;

    /**
     * Returns the last cached consumer info without making a server request.
     */
    public function cachedInfo(): ConsumerInfo;

    /**
     * Returns the consumer name.
     */
    public function name(): string;

    /**
     * Returns the deliver subject this push consumer is bound to.
     */
    public function deliverSubject(): string;

    /**
     * Unsubscribes the push consumer from its deliver subject.
     */
    public function unsubscribe(): void;

    /**
     * Drains the push consumer subscription.
     */
    public function drain(): void;
}
