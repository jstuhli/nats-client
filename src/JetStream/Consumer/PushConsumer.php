<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Message\JetStreamMsg;
use Nats\Message;
use Nats\NatsException;
use Nats\Subscription;

final class PushConsumer implements PushConsumerInterface
{
    private ?ConsumerInfo $cachedInfo = null;
    private ?Subscription $subscription = null;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $stream,
        private readonly ConsumerConfig $config,
    ) {
        if ($this->config->deliverSubject === null) {
            throw new NatsException('Push consumer requires a deliver_subject in configuration');
        }
    }

    /**
     * Subscribe to the push consumer's deliver subject with the given handler.
     */
    public function subscribe(\Closure $handler): self
    {
        $deliverSubject = $this->config->deliverSubject;
        $deliverGroup = $this->config->deliverGroup;

        $wrappedHandler = function (Message $msg) use ($handler): void {
            // Check for status messages (heartbeat, etc.)
            if ($msg->headers !== null) {
                $status = $msg->headers->get('Status');
                if ($status !== null) {
                    return;
                }
            }

            $jsMsg = new JetStreamMsg($msg, $this->js->connection());
            $handler($jsMsg);
        };

        if ($deliverGroup !== null) {
            $this->subscription = $this->js->connection()->queueSubscribe(
                $deliverSubject,
                $deliverGroup,
                $wrappedHandler,
            );
        } else {
            $this->subscription = $this->js->connection()->subscribe(
                $deliverSubject,
                $wrappedHandler,
            );
        }

        return $this;
    }

    public function info(): ConsumerInfo
    {
        $consumerName = $this->config->name ?? $this->config->durable ?? '';
        $data = $this->js->apiRequest("CONSUMER.INFO.{$this->stream}.{$consumerName}");
        $this->cachedInfo = ConsumerInfo::fromArray($data);
        return $this->cachedInfo;
    }

    public function cachedInfo(): ConsumerInfo
    {
        if ($this->cachedInfo === null) {
            return $this->info();
        }
        return $this->cachedInfo;
    }

    public function name(): string
    {
        return $this->config->name ?? $this->config->durable ?? '';
    }

    public function deliverSubject(): string
    {
        return $this->config->deliverSubject ?? '';
    }

    public function unsubscribe(): void
    {
        $this->subscription?->unsubscribe();
        $this->subscription = null;
    }

    public function drain(): void
    {
        $this->subscription?->drain();
        $this->subscription = null;
    }
}
