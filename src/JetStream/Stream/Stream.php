<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\Internal\TypeCast as T;
use Nats\JetStream\Consumer\Consumer;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumerInterface;
use Nats\JetStream\Consumer\ConsumerPauseResponse;
use Nats\JetStream\Consumer\OrderedConsumer;
use Nats\JetStream\Consumer\OrderedConsumerConfig;
use Nats\JetStream\JetStreamContext;

final class Stream implements StreamInterface
{
    private StreamInfo $cachedInfo;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $name,
        StreamInfo $info,
    ) {
        $this->cachedInfo = $info;
    }

    public function createConsumer(ConsumerConfig $config): ConsumerInterface
    {
        $data = $this->js->apiRequest(
            "CONSUMER.CREATE.{$this->name}",
            ['stream_name' => $this->name, 'config' => $config->toArray()],
        );
        return new Consumer($this->js, $this->name, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function createOrUpdateConsumer(ConsumerConfig $config): ConsumerInterface
    {
        $operation = $config->name !== null
            ? "CONSUMER.CREATE.{$this->name}.{$config->name}"
            : "CONSUMER.CREATE.{$this->name}";

        $data = $this->js->apiRequest(
            $operation,
            ['stream_name' => $this->name, 'config' => $config->toArray()],
        );
        return new Consumer($this->js, $this->name, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function updateConsumer(ConsumerConfig $config): ConsumerInterface
    {
        return $this->createOrUpdateConsumer($config);
    }

    public function consumer(string $name): ConsumerInterface
    {
        $data = $this->js->apiRequest("CONSUMER.INFO.{$this->name}.{$name}");
        return new Consumer($this->js, $this->name, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function orderedConsumer(?OrderedConsumerConfig $config = null): ConsumerInterface
    {
        return new OrderedConsumer($this->js, $this->name, $config ?? new OrderedConsumerConfig());
    }

    public function deleteConsumer(string $name): void
    {
        $this->js->apiRequest("CONSUMER.DELETE.{$this->name}.{$name}");
    }

    public function pauseConsumer(string $name, \DateTimeImmutable $pauseUntil): ConsumerPauseResponse
    {
        $data = $this->js->apiRequest(
            "CONSUMER.PAUSE.{$this->name}.{$name}",
            ['pause_until' => $pauseUntil->format(\DateTimeInterface::RFC3339)],
        );
        return ConsumerPauseResponse::fromArray($data);
    }

    public function resumeConsumer(string $name): ConsumerPauseResponse
    {
        $data = $this->js->apiRequest(
            "CONSUMER.PAUSE.{$this->name}.{$name}",
        );
        return ConsumerPauseResponse::fromArray($data);
    }

    /** @return \Generator<ConsumerConfig> */
    public function listConsumers(): \Generator
    {
        $offset = 0;
        while (true) {
            $data = $this->js->apiRequest("CONSUMER.LIST.{$this->name}", ['offset' => $offset]);
            /** @var list<mixed> $consumers */
            $consumers = $data['consumers'] ?? [];
            if ($consumers === []) {
                return;
            }
            foreach ($consumers as $consumer) {
                $consumerArr = (array) $consumer;
                yield ConsumerConfig::fromArray(T::stringKeyArray($consumerArr['config'] ?? []));
            }
            $offset += count($consumers);
            $total = T::int($data['total'] ?? 0);
            if ($offset >= $total) {
                return;
            }
        }
    }

    /** @return \Generator<string> */
    public function consumerNames(): \Generator
    {
        $offset = 0;
        while (true) {
            $data = $this->js->apiRequest("CONSUMER.NAMES.{$this->name}", ['offset' => $offset]);
            /** @var list<mixed> $names */
            $names = $data['consumers'] ?? [];
            if ($names === []) {
                return;
            }
            foreach ($names as $name) {
                yield T::string($name);
            }
            $offset += count($names);
            $total = T::int($data['total'] ?? 0);
            if ($offset >= $total) {
                return;
            }
        }
    }

    public function getMessage(int $sequence): RawStreamMessage
    {
        $data = $this->js->apiRequest(
            "STREAM.MSG.GET.{$this->name}",
            ['seq' => $sequence],
        );
        return RawStreamMessage::fromArray(T::stringKeyArray($data['message'] ?? []));
    }

    public function getLastMessageForSubject(string $subject): RawStreamMessage
    {
        $data = $this->js->apiRequest(
            "STREAM.MSG.GET.{$this->name}",
            ['last_by_subj' => $subject],
        );
        return RawStreamMessage::fromArray(T::stringKeyArray($data['message'] ?? []));
    }

    public function deleteMessage(int $sequence): void
    {
        $this->js->apiRequest(
            "STREAM.MSG.DELETE.{$this->name}",
            ['seq' => $sequence],
        );
    }

    public function secureDeleteMessage(int $sequence): void
    {
        $this->js->apiRequest(
            "STREAM.MSG.DELETE.{$this->name}",
            ['seq' => $sequence, 'no_erase' => false],
        );
    }

    public function purge(?StreamPurgeOptions $options = null): int
    {
        $payload = $options !== null ? $options->toArray() : null;
        if ($payload === []) { $payload = null; }
        $data = $this->js->apiRequest("STREAM.PURGE.{$this->name}", $payload);
        return T::int($data['purged'] ?? 0);
    }

    public function info(bool $deletedDetails = false, ?string $subjectFilter = null): StreamInfo
    {
        $payload = null;
        if ($deletedDetails || $subjectFilter !== null) {
            $payload = [];
            if ($deletedDetails) { $payload['deleted_details'] = true; }
            if ($subjectFilter !== null) { $payload['subjects_filter'] = $subjectFilter; }
        }
        $data = $this->js->apiRequest("STREAM.INFO.{$this->name}", $payload);
        $this->cachedInfo = StreamInfo::fromArray($data);
        return $this->cachedInfo;
    }

    public function cachedInfo(): StreamInfo
    {
        return $this->cachedInfo;
    }

    /**
     * Directly get a message from the stream using the DIRECT.GET API.
     * More efficient than getMessage() as it bypasses the JetStream API layer.
     * Requires allowDirect: true on the stream config.
     *
     * Matches Go client Stream.GetMsg with direct get subjects.
     *
     * @param string $subject Get the last message for this subject
     * @param int|null $sequence If provided, get the message at this specific sequence instead
     */
    public function directGet(string $subject, ?int $sequence = null): RawStreamMessage
    {
        $conn = $this->js->connection();

        if ($sequence !== null) {
            $payload = json_encode(['seq' => $sequence], JSON_THROW_ON_ERROR);
        } else {
            $payload = json_encode(['last_by_subj' => $subject], JSON_THROW_ON_ERROR);
        }

        $reply = $conn->request(
            "\$JS.API.DIRECT.GET.{$this->name}",
            $payload,
            5.0,
        );

        // Check for error status
        if ($reply->headers !== null) {
            $status = $reply->headers->get('Status');
            if ($status !== null && str_starts_with(trim($status), '404')) {
                throw new \Nats\NatsException("No message found");
            }
        }

        // Parse response: headers contain metadata, body is the message data
        $msgSubject = $reply->headers?->get('Nats-Subject') ?? $subject;
        $msgSeq = (int) ($reply->headers?->get('Nats-Sequence') ?? 0);
        $msgTime = $reply->headers?->get('Nats-Time-Stamp');

        return new RawStreamMessage(
            subject: $msgSubject,
            sequence: $msgSeq,
            data: $reply->data,
            time: $msgTime !== null ? new \DateTimeImmutable($msgTime) : null,
            headers: $reply->headers,
        );
    }
}
