<?php

declare(strict_types=1);

namespace Nats\JetStream\Consumer;

use Nats\Headers;
use Nats\Inbox;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Message\JetStreamMessage;
use Nats\JetStream\Message\JetStreamMsg;
use Nats\TimeoutException;

final class Consumer implements ConsumerInterface
{
    private ?ConsumerInfo $cachedInfo = null;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $stream,
        private readonly ConsumerConfig $config,
    ) {}

    public function fetch(int $batch, ?FetchOptions $opts = null): MessageBatch
    {
        $opts ??= new FetchOptions();
        $messageBatch = new MessageBatch();

        $inbox = Inbox::generate();
        $sub = $this->js->connection()->subscribeSync($inbox);

        // Send pull request
        $expiresNs = (int) ($opts->timeout * 1_000_000_000);
        $requestData = [
            'batch' => $batch,
            'expires' => $expiresNs,
        ];
        if ($opts->heartbeat !== null) {
            $requestData['idle_heartbeat'] = (int) ($opts->heartbeat * 1_000_000_000);
        }
        if ($opts->minPending !== null) {
            $requestData['min_pending'] = $opts->minPending;
        }
        if ($opts->minAckPending !== null) {
            $requestData['min_ack_pending'] = $opts->minAckPending;
        }
        $request = json_encode($requestData, JSON_THROW_ON_ERROR);

        $consumerName = $this->config->name ?? $this->config->durable ?? '';
        $this->js->connection()->publish(
            "\$JS.API.CONSUMER.MSG.NEXT.{$this->stream}.{$consumerName}",
            $request,
            $inbox,
        );

        $received = 0;
        $deadline = microtime(true) + $opts->timeout;

        while ($received < $batch && microtime(true) < $deadline) {
            try {
                $remaining = max(0.001, $deadline - microtime(true));
                $msg = $sub->nextMessage($remaining);

                // Check for status messages (heartbeat, end of batch, etc.)
                if ($msg->headers !== null) {
                    $status = $msg->headers->get('Status');
                    if ($status !== null) {
                        $statusCode = (int) trim(explode(' ', $status)[0]);
                        if ($statusCode === 404 || $statusCode === 408 || $statusCode === 409) {
                            break;
                        }
                        // 100 = heartbeat/idle, skip
                        continue;
                    }
                }

                $jsMsg = new JetStreamMsg($msg, $this->js->connection());
                $messageBatch->addMessage($jsMsg);
                $received++;
            } catch (TimeoutException) {
                break;
            }
        }

        $sub->unsubscribe();
        $messageBatch->setComplete();

        return $messageBatch;
    }

    public function fetchBytes(int $maxBytes, ?FetchOptions $opts = null): MessageBatch
    {
        $opts ??= new FetchOptions();
        $messageBatch = new MessageBatch();

        $inbox = Inbox::generate();
        $sub = $this->js->connection()->subscribeSync($inbox);

        $expiresNs = (int) ($opts->timeout * 1_000_000_000);
        $request = json_encode([
            'max_bytes' => $maxBytes,
            'expires' => $expiresNs,
        ], JSON_THROW_ON_ERROR);

        $consumerName = $this->config->name ?? $this->config->durable ?? '';
        $this->js->connection()->publish(
            "\$JS.API.CONSUMER.MSG.NEXT.{$this->stream}.{$consumerName}",
            $request,
            $inbox,
        );

        $totalBytes = 0;
        $deadline = microtime(true) + $opts->timeout;

        while ($totalBytes < $maxBytes && microtime(true) < $deadline) {
            try {
                $remaining = max(0.001, $deadline - microtime(true));
                $msg = $sub->nextMessage($remaining);

                if ($msg->headers !== null) {
                    $status = $msg->headers->get('Status');
                    if ($status !== null) {
                        $statusCode = (int) trim(explode(' ', $status)[0]);
                        if ($statusCode >= 400) { break; }
                        continue;
                    }
                }

                $jsMsg = new JetStreamMsg($msg, $this->js->connection());
                $messageBatch->addMessage($jsMsg);
                $totalBytes += strlen($msg->data);
            } catch (TimeoutException) {
                break;
            }
        }

        $sub->unsubscribe();
        $messageBatch->setComplete();

        return $messageBatch;
    }

    public function fetchNoWait(int $batch): MessageBatch
    {
        $messageBatch = new MessageBatch();

        $inbox = Inbox::generate();
        $sub = $this->js->connection()->subscribeSync($inbox);

        $request = json_encode([
            'batch' => $batch,
            'no_wait' => true,
        ], JSON_THROW_ON_ERROR);

        $consumerName = $this->config->name ?? $this->config->durable ?? '';
        $this->js->connection()->publish(
            "\$JS.API.CONSUMER.MSG.NEXT.{$this->stream}.{$consumerName}",
            $request,
            $inbox,
        );

        // Quick drain - short timeout since no_wait returns immediately
        $deadline = microtime(true) + 1.0;
        $received = 0;

        while ($received < $batch && microtime(true) < $deadline) {
            try {
                $msg = $sub->nextMessage(0.5);

                if ($msg->headers !== null) {
                    $status = $msg->headers->get('Status');
                    if ($status !== null) {
                        break;
                    }
                }

                $jsMsg = new JetStreamMsg($msg, $this->js->connection());
                $messageBatch->addMessage($jsMsg);
                $received++;
            } catch (TimeoutException) {
                break;
            }
        }

        $sub->unsubscribe();
        $messageBatch->setComplete();

        return $messageBatch;
    }

    public function consume(\Closure $handler, ?ConsumeOptions $opts = null): ConsumeContext
    {
        $opts ??= new ConsumeOptions();
        $ctx = new ConsumeContext($this, $handler, $opts);
        return $ctx;
    }

    public function messages(?ConsumeOptions $opts = null): MessagesContext
    {
        $opts ??= new ConsumeOptions();
        return new MessagesContext($this, $opts);
    }

    public function next(float $timeout = 5.0): JetStreamMessage
    {
        $batch = $this->fetch(1, new FetchOptions(timeout: $timeout));
        foreach ($batch->messages() as $msg) {
            return $msg;
        }
        throw new TimeoutException('No message available');
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
}
