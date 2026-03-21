<?php

declare(strict_types=1);

namespace Nats\JetStream;

use Nats\Connection;
use Nats\Enum\StoreCompression;
use Nats\Headers;
use Nats\Internal\TypeCast as T;
use Nats\JetStream\Consumer\Consumer;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumerInfo;
use Nats\JetStream\Consumer\ConsumerInterface;
use Nats\JetStream\Consumer\OrderedConsumer;
use Nats\JetStream\Consumer\OrderedConsumerConfig;
use Nats\JetStream\Consumer\PushConsumer;
use Nats\JetStream\Consumer\PushConsumerInterface;
use Nats\JetStream\Error\JetStreamException;
use Nats\JetStream\Publish\PubAck;
use Nats\JetStream\Publish\PubAckFuture;
use Nats\JetStream\Publish\PublishOptions;
use Nats\JetStream\Stream\Stream;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamInfo;
use Nats\JetStream\Stream\StreamInterface;
use Nats\Message;
use Nats\NatsException;

final class JetStreamContext
{
    /** @var list<PubAckFuture> */
    private array $pendingFutures = [];

    /** @internal */
    public function __construct(
        private readonly Connection $conn,
        private readonly JetStreamOptions $options,
    ) {}

    public function connection(): Connection
    {
        return $this->conn;
    }

    // --- Stream Management ---

    /**
     * @throws JetStreamException If the stream cannot be created
     */
    public function createStream(StreamConfig $config): StreamInterface
    {
        $data = $this->apiRequest('STREAM.CREATE.' . $config->name, $config->toArray());
        $info = StreamInfo::fromArray($data);
        return new Stream($this, $config->name, $info);
    }

    /**
     * @throws JetStreamException If the stream cannot be updated
     */
    public function updateStream(StreamConfig $config): StreamInterface
    {
        $data = $this->apiRequest('STREAM.UPDATE.' . $config->name, $config->toArray());
        $info = StreamInfo::fromArray($data);
        return new Stream($this, $config->name, $info);
    }

    /**
     * @throws JetStreamException If the stream cannot be created or updated
     */
    public function createOrUpdateStream(StreamConfig $config): StreamInterface
    {
        $data = $this->apiRequest('STREAM.CREATE.' . $config->name, $config->toArray());
        $info = StreamInfo::fromArray($data);
        return new Stream($this, $config->name, $info);
    }

    /**
     * @throws JetStreamException If the stream is not found
     */
    public function stream(string $name): StreamInterface
    {
        $data = $this->apiRequest("STREAM.INFO.{$name}");
        $info = StreamInfo::fromArray($data);
        return new Stream($this, $name, $info);
    }

    /**
     * @throws JetStreamException If the stream cannot be deleted
     */
    public function deleteStream(string $name): void
    {
        $this->apiRequest("STREAM.DELETE.{$name}");
    }

    /**
     * @return \Generator<StreamInfo>
     * @throws JetStreamException If the API request fails
     */
    public function listStreams(?string $subject = null): \Generator
    {
        $offset = 0;
        while (true) {
            $payload = ['offset' => $offset];
            if ($subject !== null) { $payload['subject'] = $subject; }
            $data = $this->apiRequest('STREAM.LIST', $payload);
            /** @var list<mixed> $streams */
            $streams = $data['streams'] ?? [];
            if ($streams === []) { return; }
            foreach ($streams as $stream) {
                yield StreamInfo::fromArray(T::stringKeyArray($stream));
            }
            $offset += count($streams);
            if ($offset >= T::int($data['total'] ?? 0)) { return; }
        }
    }

    /**
     * @throws JetStreamException If no stream is found for the subject
     */
    public function streamNameBySubject(string $subject): string
    {
        $data = $this->apiRequest('STREAM.NAMES', ['subject' => $subject]);
        /** @var list<mixed> $names */
        $names = $data['streams'] ?? [];
        if ($names === []) {
            throw new JetStreamException('No stream found for subject: ' . $subject);
        }
        return T::string($names[0]);
    }

    /** @return \Generator<string> */
    public function streamNames(?string $subject = null): \Generator
    {
        $offset = 0;
        while (true) {
            $payload = ['offset' => $offset];
            if ($subject !== null) { $payload['subject'] = $subject; }
            $data = $this->apiRequest('STREAM.NAMES', $payload);
            /** @var list<mixed> $names */
            $names = $data['streams'] ?? [];
            if ($names === []) { return; }
            foreach ($names as $name) { yield T::string($name); }
            $offset += count($names);
            if ($offset >= T::int($data['total'] ?? 0)) { return; }
        }
    }

    // --- Consumer Management ---

    /**
     * @throws JetStreamException If the consumer cannot be created
     */
    public function createConsumer(string $stream, ConsumerConfig $config): ConsumerInterface
    {
        $data = $this->apiRequest(
            "CONSUMER.CREATE.{$stream}",
            ['stream_name' => $stream, 'config' => $config->toArray()],
        );
        return new Consumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    /**
     * @throws JetStreamException If the consumer cannot be created or updated
     */
    public function createOrUpdateConsumer(string $stream, ConsumerConfig $config): ConsumerInterface
    {
        $op = $config->name !== null
            ? "CONSUMER.CREATE.{$stream}.{$config->name}"
            : "CONSUMER.CREATE.{$stream}";
        $data = $this->apiRequest($op, ['stream_name' => $stream, 'config' => $config->toArray()]);
        return new Consumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    /**
     * @throws JetStreamException If the consumer is not found
     */
    public function consumer(string $stream, string $name): ConsumerInterface
    {
        $data = $this->apiRequest("CONSUMER.INFO.{$stream}.{$name}");
        return new Consumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function orderedConsumer(string $stream, ?OrderedConsumerConfig $config = null): ConsumerInterface
    {
        return new OrderedConsumer($this, $stream, $config ?? new OrderedConsumerConfig());
    }

    /**
     * @throws JetStreamException If the consumer cannot be deleted
     */
    public function deleteConsumer(string $stream, string $name): void
    {
        $this->apiRequest("CONSUMER.DELETE.{$stream}.{$name}");
    }

    // --- Push Consumer Management ---

    public function createPushConsumer(string $stream, ConsumerConfig $config): PushConsumerInterface
    {
        $data = $this->apiRequest(
            "CONSUMER.CREATE.{$stream}",
            ['stream_name' => $stream, 'config' => $config->toArray()],
        );
        return new PushConsumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function updatePushConsumer(string $stream, ConsumerConfig $config): PushConsumerInterface
    {
        $consumerName = $config->name ?? $config->durable ?? '';
        $data = $this->apiRequest(
            "CONSUMER.CREATE.{$stream}.{$consumerName}",
            ['stream_name' => $stream, 'config' => $config->toArray()],
        );
        return new PushConsumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function createOrUpdatePushConsumer(string $stream, ConsumerConfig $config): PushConsumerInterface
    {
        $op = $config->name !== null
            ? "CONSUMER.CREATE.{$stream}.{$config->name}"
            : "CONSUMER.CREATE.{$stream}";
        $data = $this->apiRequest($op, ['stream_name' => $stream, 'config' => $config->toArray()]);
        return new PushConsumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    public function pushConsumer(string $stream, string $name): PushConsumerInterface
    {
        $data = $this->apiRequest("CONSUMER.INFO.{$stream}.{$name}");
        return new PushConsumer($this, $stream, ConsumerConfig::fromArray(T::stringKeyArray($data['config'] ?? [])));
    }

    // --- Publishing ---

    /**
     * @throws JetStreamException If the server rejects the publish
     * @throws NatsException If not connected
     * @throws \JsonException If payload encoding fails
     */
    public function publish(string $subject, string $data = '', PublishOptions ...$opts): PubAck
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, array_values($opts));

        $retryAttempts = 1;
        $retryWait = 0.25;
        foreach ($opts as $opt) {
            if ($opt->type === 'retry_attempts') { $retryAttempts = T::int($opt->value); }
            if ($opt->type === 'retry_wait') { $retryWait = T::float($opt->value); }
        }

        $lastError = null;
        for ($attempt = 0; $attempt <= $retryAttempts; $attempt++) {
            if ($attempt > 0) {
                usleep((int) ($retryWait * 1_000_000));
            }

            try {
                $msg = $headers->count() > 0
                    ? new Message(subject: $subject, data: $data, headers: $headers)
                    : new Message(subject: $subject, data: $data);

                $reply = $headers->count() > 0
                    ? $this->conn->requestMessage($msg, $this->options->timeout)
                    : $this->conn->request($subject, $data, $this->options->timeout);

                $response = T::stringKeyArray(json_decode($reply->data, true, 512, JSON_THROW_ON_ERROR));

                if (isset($response['error'])) {
                    throw JetStreamException::fromApiResponse($response);
                }

                return PubAck::fromArray($response);
            } catch (\Throwable $e) {
                $lastError = $e;
            }
        }

        throw $lastError ?? new NatsException('Publish failed');
    }

    /**
     * @throws JetStreamException If the server rejects the publish
     * @throws NatsException If not connected
     * @throws \JsonException If payload encoding fails
     */
    public function publishMessage(Message $msg, PublishOptions ...$opts): PubAck
    {
        $headers = $msg->headers ?? new Headers();
        PublishOptions::applyToHeaders($headers, array_values($opts));

        $request = new Message(
            subject: $msg->subject,
            data: $msg->data,
            headers: $headers,
        );

        $reply = $this->conn->requestMessage($request, $this->options->timeout);
        $response = T::stringKeyArray(json_decode($reply->data, true, 512, JSON_THROW_ON_ERROR));

        if (isset($response['error'])) {
            throw JetStreamException::fromApiResponse($response);
        }

        return PubAck::fromArray($response);
    }

    public function publishAsync(string $subject, string $data = '', PublishOptions ...$opts): PubAckFuture
    {
        $headers = new Headers();
        PublishOptions::applyToHeaders($headers, array_values($opts));

        $msg = $headers->count() > 0
            ? new Message(subject: $subject, data: $data, headers: $headers)
            : new Message(subject: $subject, data: $data);

        $future = new PubAckFuture($msg);
        $this->pendingFutures[] = $future;

        $inbox = $this->conn->newInbox();
        $sub = $this->conn->subscribe($inbox, function (Message $reply) use ($future, &$sub): void {
            try {
                $response = T::stringKeyArray(json_decode($reply->data, true, 512, JSON_THROW_ON_ERROR));
                if (isset($response['error'])) {
                    $future->reject(JetStreamException::fromApiResponse($response));
                } else {
                    $future->resolve(PubAck::fromArray($response));
                }
            } catch (\Throwable $e) {
                $future->reject($e);
            }
            if ($sub !== null) { $sub->unsubscribe(); }
        });
        $sub->autoUnsubscribe(1);

        $this->conn->publish($subject, $data, $inbox);

        return $future;
    }

    /**
     * Publish a Message object asynchronously with headers support.
     * Matches Go client PublishMsgAsync.
     */
    public function publishAsyncMessage(Message $msg, PublishOptions ...$opts): PubAckFuture
    {
        $headers = $msg->headers !== null ? clone $msg->headers : new Headers();
        PublishOptions::applyToHeaders($headers, array_values($opts));

        $publishMsg = $headers->count() > 0
            ? new Message(subject: $msg->subject, data: $msg->data, headers: $headers)
            : new Message(subject: $msg->subject, data: $msg->data);

        $future = new PubAckFuture($publishMsg);
        $this->pendingFutures[] = $future;

        $inbox = $this->conn->newInbox();
        $sub = $this->conn->subscribe($inbox, function (Message $reply) use ($future, &$sub): void {
            try {
                $response = T::stringKeyArray(json_decode($reply->data, true, 512, JSON_THROW_ON_ERROR));
                if (isset($response['error'])) {
                    $future->reject(JetStreamException::fromApiResponse($response));
                } else {
                    $future->resolve(PubAck::fromArray($response));
                }
            } catch (\Throwable $e) {
                $future->reject($e);
            }
            if ($sub !== null) { $sub->unsubscribe(); }
        });
        $sub->autoUnsubscribe(1);

        $this->conn->publishMessage(new Message(
            subject: $msg->subject,
            data: $msg->data,
            replyTo: $inbox,
            headers: $headers->count() > 0 ? $headers : null,
        ));

        return $future;
    }

    /**
     * Returns the number of pending (incomplete) async publish operations.
     */
    public function publishAsyncPending(): int
    {
        $pending = 0;
        foreach ($this->pendingFutures as $future) {
            if (!$future->isComplete()) {
                $pending++;
            }
        }
        return $pending;
    }

    /**
     * Returns true if all tracked async publish operations have completed.
     */
    public function publishAsyncComplete(): bool
    {
        foreach ($this->pendingFutures as $future) {
            if (!$future->isComplete()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Remove completed futures from internal tracking to free memory.
     */
    public function cleanupPublisher(): void
    {
        $this->pendingFutures = array_values(array_filter(
            $this->pendingFutures,
            static fn(PubAckFuture $f) => !$f->isComplete(),
        ));
    }

    // --- KeyValue ---

    public function createKeyValue(\Nats\KeyValue\KeyValueConfig $config): \Nats\KeyValue\KeyValue
    {
        $streamConfig = $this->buildKvStreamConfig($config);
        $this->createStream($streamConfig);
        return new \Nats\KeyValue\KeyValue($this, $config->bucket);
    }

    public function createOrUpdateKeyValue(\Nats\KeyValue\KeyValueConfig $config): \Nats\KeyValue\KeyValue
    {
        $streamConfig = $this->buildKvStreamConfig($config);

        try {
            $this->updateStream($streamConfig);
        } catch (JetStreamException) {
            $this->createStream($streamConfig);
        }

        return new \Nats\KeyValue\KeyValue($this, $config->bucket);
    }

    public function updateKeyValue(\Nats\KeyValue\KeyValueConfig $config): \Nats\KeyValue\KeyValue
    {
        $streamConfig = $this->buildKvStreamConfig($config);
        $this->updateStream($streamConfig);
        return new \Nats\KeyValue\KeyValue($this, $config->bucket);
    }

    public function keyValue(string $bucket): \Nats\KeyValue\KeyValue
    {
        // Verify stream exists
        $this->apiRequest("STREAM.INFO.KV_{$bucket}");
        return new \Nats\KeyValue\KeyValue($this, $bucket);
    }

    public function deleteKeyValue(string $bucket): void
    {
        $this->deleteStream("KV_{$bucket}");
    }

    /** @return \Generator<string> */
    public function keyValueStoreNames(): \Generator
    {
        foreach ($this->streamNames() as $name) {
            if (str_starts_with($name, 'KV_')) {
                yield substr($name, 3);
            }
        }
    }

    /** @return \Generator<\Nats\KeyValue\KeyValueStatus> */
    public function keyValueStores(): \Generator
    {
        foreach ($this->keyValueStoreNames() as $bucket) {
            $kv = new \Nats\KeyValue\KeyValue($this, $bucket);
            yield $kv->status();
        }
    }

    // --- ObjectStore ---

    public function createObjectStore(\Nats\ObjectStore\ObjectStoreConfig $config): \Nats\ObjectStore\ObjectStore
    {
        $streamConfig = $this->buildObjStreamConfig($config);
        $this->createStream($streamConfig);
        return new \Nats\ObjectStore\ObjectStore($this, $config->bucket, $config->maxChunkSize ?? 131072);
    }

    public function createOrUpdateObjectStore(\Nats\ObjectStore\ObjectStoreConfig $config): \Nats\ObjectStore\ObjectStore
    {
        $streamConfig = $this->buildObjStreamConfig($config);

        try {
            $this->updateStream($streamConfig);
        } catch (JetStreamException) {
            $this->createStream($streamConfig);
        }

        return new \Nats\ObjectStore\ObjectStore($this, $config->bucket, $config->maxChunkSize ?? 131072);
    }

    public function updateObjectStore(\Nats\ObjectStore\ObjectStoreConfig $config): \Nats\ObjectStore\ObjectStore
    {
        $streamConfig = $this->buildObjStreamConfig($config);
        $this->updateStream($streamConfig);
        return new \Nats\ObjectStore\ObjectStore($this, $config->bucket, $config->maxChunkSize ?? 131072);
    }

    public function objectStore(string $bucket): \Nats\ObjectStore\ObjectStore
    {
        $this->apiRequest("STREAM.INFO.OBJ_{$bucket}");
        return new \Nats\ObjectStore\ObjectStore($this, $bucket);
    }

    public function deleteObjectStore(string $bucket): void
    {
        $this->deleteStream("OBJ_{$bucket}");
    }

    /** @return \Generator<string> */
    public function objectStoreNames(): \Generator
    {
        foreach ($this->streamNames() as $name) {
            if (str_starts_with($name, 'OBJ_')) {
                yield substr($name, 4);
            }
        }
    }

    /** @return \Generator<\Nats\ObjectStore\ObjectStoreStatus> */
    public function objectStores(): \Generator
    {
        foreach ($this->objectStoreNames() as $bucket) {
            $os = new \Nats\ObjectStore\ObjectStore($this, $bucket);
            yield $os->status();
        }
    }

    // --- Account ---

    public function accountInfo(): AccountInfo
    {
        $data = $this->apiRequest('INFO');
        return AccountInfo::fromArray($data);
    }

    // --- Internal API ---

    /**
     * Make a JetStream API request.
     *
     * @internal
     * @param array<string, mixed>|null $payload
     * @return array<string, mixed>
     */
    public function apiRequest(string $operation, ?array $payload = null): array
    {
        $subject = $this->options->apiSubject($operation);
        $data = $payload !== null ? json_encode($payload, JSON_THROW_ON_ERROR) : '';

        $reply = $this->conn->request($subject, $data, $this->options->timeout);
        $response = T::stringKeyArray(json_decode($reply->data, true, 512, JSON_THROW_ON_ERROR));

        if (isset($response['error'])) {
            throw JetStreamException::fromApiResponse($response);
        }

        return $response;
    }

    // --- Private helpers ---

    private function buildKvStreamConfig(\Nats\KeyValue\KeyValueConfig $config): StreamConfig
    {
        $streamName = "KV_{$config->bucket}";

        return new StreamConfig(
            name: $streamName,
            subjects: ["\$KV.{$config->bucket}.>"],
            description: $config->description,
            maxAge: $config->ttl !== null ? (int) $config->ttl : null,
            maxBytes: $config->maxBytes,
            maxMsgSize: $config->maxValueSize,
            maxMessagesPerSubject: $config->history,
            storage: \Nats\Enum\StorageType::File,
            discard: \Nats\Enum\DiscardPolicy::New,
            replicas: $config->replicas,
            mirror: $config->mirror,
            sources: $config->sources,
            rePublish: $config->rePublish,
            placement: $config->placement,
            compression: $config->compression ? StoreCompression::S2 : StoreCompression::None,
            allowRollup: true,
            denyDelete: true,
            denyPurge: false,
            discardNewPerSubject: true,
            allowDirect: true,
            metadata: $config->metadata,
            allowMsgTtl: $config->allowMsgTtl,
        );
    }

    private function buildObjStreamConfig(\Nats\ObjectStore\ObjectStoreConfig $config): StreamConfig
    {
        $streamName = "OBJ_{$config->bucket}";
        $chunkSubject = "\$O.{$config->bucket}.C.>";
        $metaSubject = "\$O.{$config->bucket}.M.>";

        return new StreamConfig(
            name: $streamName,
            subjects: [$chunkSubject, $metaSubject],
            description: $config->description,
            maxAge: $config->ttl !== null ? (int) $config->ttl : null,
            maxBytes: $config->maxBytes,
            storage: $config->storage,
            replicas: $config->replicas,
            placement: $config->placement,
            compression: $config->compression ? StoreCompression::S2 : StoreCompression::None,
            discardNewPerSubject: false,
            allowRollup: true,
            metadata: $config->metadata,
        );
    }
}
