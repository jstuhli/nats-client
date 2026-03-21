<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\JetStream\Message\JetStreamMessage;

interface ConsumerInterface
{
    public function fetch(int $batch, ?FetchOptions $opts = null): MessageBatch;

    public function fetchBytes(int $maxBytes, ?FetchOptions $opts = null): MessageBatch;

    public function fetchNoWait(int $batch): MessageBatch;

    public function consume(\Closure $handler, ?ConsumeOptions $opts = null): ConsumeContext;

    public function messages(?ConsumeOptions $opts = null): MessagesContext;

    public function next(float $timeout = 5.0): JetStreamMessage;

    public function info(): ConsumerInfo;

    public function cachedInfo(): ConsumerInfo;
}
