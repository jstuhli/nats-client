<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Inbox;
use Nats\Internal\TypeCast as T;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Message\JetStreamMessage;
use Nats\NatsException;

final class OrderedConsumer implements ConsumerInterface
{
    private ?Consumer $inner = null;
    private int $cursor = 0;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $stream,
        private readonly OrderedConsumerConfig $config,
    ) {}

    public function fetch(int $batch, ?FetchOptions $opts = null): MessageBatch
    {
        return $this->getInner()->fetch($batch, $opts);
    }

    public function fetchBytes(int $maxBytes, ?FetchOptions $opts = null): MessageBatch
    {
        return $this->getInner()->fetchBytes($maxBytes, $opts);
    }

    public function fetchNoWait(int $batch): MessageBatch
    {
        return $this->getInner()->fetchNoWait($batch);
    }

    public function consume(\Closure $handler, ?ConsumeOptions $opts = null): ConsumeContext
    {
        return $this->getInner()->consume($handler, $opts);
    }

    public function messages(?ConsumeOptions $opts = null): MessagesContext
    {
        return $this->getInner()->messages($opts);
    }

    public function next(float $timeout = 5.0): JetStreamMessage
    {
        return $this->getInner()->next($timeout);
    }

    public function info(): ConsumerInfo
    {
        return $this->getInner()->info();
    }

    public function cachedInfo(): ConsumerInfo
    {
        return $this->info();
    }

    private function getInner(): Consumer
    {
        if ($this->inner === null) {
            $this->createOrderedConsumer();
        }
        if ($this->inner === null) {
            throw new NatsException('Failed to create ordered consumer');
        }
        return $this->inner;
    }

    private function createOrderedConsumer(): void
    {
        $consumerConfig = new ConsumerConfig(
            name: Inbox::nuid(),
            deliverPolicy: $this->cursor > 0
                ? \Nats\Enum\DeliveryPolicy::ByStartSequence
                : $this->config->deliverPolicy,
            optStartSeq: $this->cursor > 0 ? $this->cursor + 1 : $this->config->optStartSeq,
            optStartTime: $this->config->optStartTime,
            ackPolicy: \Nats\Enum\AckPolicy::None,
            maxDeliver: 1,
            maxAckPending: 0,
            headersOnly: $this->config->headersOnly,
            filterSubject: $this->config->filterSubject,
            filterSubjects: $this->config->filterSubjects,
            replayPolicy: $this->config->replayPolicy,
            memoryStorage: true,
            inactiveThreshold: 30.0,
        );

        $data = $this->js->apiRequest(
            "CONSUMER.CREATE.{$this->stream}",
            ['stream_name' => $this->stream, 'config' => $consumerConfig->toArray()],
        );

        $this->inner = new Consumer(
            $this->js,
            $this->stream,
            ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])),
        );
    }
}
