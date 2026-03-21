<?php

declare(strict_types=1);

namespace Nats\KeyValue;

use Nats\Enum\KeyValueOperation;
use Nats\Headers;
use Nats\JetStream\Error\JetStreamException;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Message\JetStreamMessage;
use Nats\JetStream\Publish\PublishOptions;
use Nats\Message;
use Nats\NatsException;

final class KeyValue implements KeyValueInterface
{
    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $bucketName,
    ) {}

    /**
     * @throws NatsException If the key is invalid or the operation fails
     */
    public function put(string $key, string $value): int
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";
        $ack = $this->js->publish($subject, $value);
        return (int) $ack->sequence;
    }

    /**
     * @throws NatsException If the key is not found or has been deleted
     */
    public function get(string $key): KeyValueEntry
    {
        $this->validateKey($key);

        try {
            $stream = $this->js->stream("KV_{$this->bucketName}");
            $rawMsg = $stream->getLastMessageForSubject("\$KV.{$this->bucketName}.{$key}");
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10037) { // No message found
                throw new NatsException("Key not found: {$key}");
            }
            throw $e;
        }

        $operation = KeyValueOperation::Put;
        if ($rawMsg->headers !== null) {
            $kvOp = $rawMsg->headers->get('KV-Operation');
            if ($kvOp !== null) {
                $operation = KeyValueOperation::from($kvOp);
            }
        }

        if ($operation !== KeyValueOperation::Put) {
            throw new NatsException("Key not found: {$key}");
        }

        return new KeyValueEntry(
            key: $key,
            value: $rawMsg->data,
            revision: $rawMsg->sequence,
            delta: 0,
            operation: $operation,
            timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
            bucket: $this->bucketName,
        );
    }

    /**
     * @throws NatsException If the key or revision is not found
     */
    public function getRevision(string $key, int $revision): KeyValueEntry
    {
        $this->validateKey($key);

        $stream = $this->js->stream("KV_{$this->bucketName}");

        try {
            $rawMsg = $stream->getMessage($revision);
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10037) {
                throw new NatsException("Revision not found: {$revision}");
            }
            throw $e;
        }

        // Verify the message subject matches the expected key
        $expectedSubject = "\$KV.{$this->bucketName}.{$key}";
        if ($rawMsg->subject !== $expectedSubject) {
            throw new NatsException("Revision {$revision} does not belong to key: {$key}");
        }

        $operation = KeyValueOperation::Put;
        if ($rawMsg->headers !== null) {
            $kvOp = $rawMsg->headers->get('KV-Operation');
            if ($kvOp !== null) {
                $operation = KeyValueOperation::from($kvOp);
            }
        }

        return new KeyValueEntry(
            key: $key,
            value: $rawMsg->data,
            revision: $rawMsg->sequence,
            delta: 0,
            operation: $operation,
            timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
            bucket: $this->bucketName,
        );
    }

    public function create(string $key, string $value, ?float $keyTtl = null): int
    {
        $this->validateKey($key);

        $headers = new Headers();
        $headers->set('Nats-Expected-Last-Subject-Sequence', '0');

        if ($keyTtl !== null) {
            $headers->set('Nats-TTL', (string) ((int) ($keyTtl * 1_000_000_000)));
        }

        $subject = "\$KV.{$this->bucketName}.{$key}";
        $msg = new Message(subject: $subject, data: $value, headers: $headers);

        try {
            $ack = $this->js->publishMessage($msg);
            return (int) $ack->sequence;
        } catch (JetStreamException $e) {
            if ($e->apiError?->errCode === 10071) { // Wrong last sequence
                throw new NatsException("Key already exists: {$key}");
            }
            throw $e;
        }
    }

    public function update(string $key, string $value, int $revision): int
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";
        $ack = $this->js->publish($subject, $value, PublishOptions::expectLastSequencePerSubject($revision));
        return (int) $ack->sequence;
    }

    public function delete(string $key, ?int $lastRevision = null): void
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";

        $headers = new Headers();
        $headers->set('KV-Operation', KeyValueOperation::Delete->value);

        $opts = [];
        if ($lastRevision !== null) {
            $opts[] = PublishOptions::expectLastSequencePerSubject($lastRevision);
        }

        $msg = new Message(subject: $subject, data: '', headers: $headers);
        $this->js->publishMessage($msg, ...$opts);
    }

    public function purge(string $key, ?float $ttl = null): void
    {
        $this->validateKey($key);
        $subject = "\$KV.{$this->bucketName}.{$key}";

        $headers = new Headers();
        $headers->set('KV-Operation', KeyValueOperation::Purge->value);
        $headers->set('Nats-Rollup', 'sub');

        if ($ttl !== null) {
            $headers->set('Nats-TTL', (string) ((int) ($ttl * 1_000_000_000)));
        }

        $msg = new Message(subject: $subject, data: '', headers: $headers);
        $this->js->publishMessage($msg);
    }

    public function purgeDeletes(?float $ttl = null): void
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $stream->purge();
    }

    public function listKeys(): \Generator
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $consumer = $stream->orderedConsumer();
        $batch = $consumer->fetch(1000);

        $seenKeys = [];
        foreach ($batch->messages() as $msg) {
            $subject = $msg->subject();
            $key = substr($subject, strlen("\$KV.{$this->bucketName}."));

            // Only yield Put operations, skip deletes
            $operation = KeyValueOperation::Put;
            $headers = $msg->headers();
            if ($headers !== null) {
                $kvOp = $headers->get('KV-Operation');
                if ($kvOp !== null) {
                    $operation = KeyValueOperation::from($kvOp);
                }
            }

            if ($operation === KeyValueOperation::Put && !isset($seenKeys[$key])) {
                $seenKeys[$key] = true;
                yield $key;
            }
        }
    }

    public function listKeysFiltered(string ...$filters): \Generator
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $streamInfo = $stream->info();
        $prefix = "\$KV.{$this->bucketName}.";

        // Build full subject patterns for matching
        $patterns = array_map(
            fn(string $f) => $prefix . $f,
            $filters,
        );

        $seenKeys = [];
        for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
            try {
                $rawMsg = $stream->getMessage($seq);
            } catch (\Throwable) {
                continue;
            }

            // Check if subject matches any of the filter patterns
            $matches = false;
            foreach ($patterns as $pattern) {
                if ($this->subjectMatchesFilter($rawMsg->subject, $pattern)) {
                    $matches = true;
                    break;
                }
            }
            if (!$matches) {
                continue;
            }

            $key = substr($rawMsg->subject, strlen($prefix));

            $operation = KeyValueOperation::Put;
            if ($rawMsg->headers !== null) {
                $kvOp = $rawMsg->headers->get('KV-Operation');
                if ($kvOp !== null) {
                    $operation = KeyValueOperation::from($kvOp);
                }
            }

            if ($operation === KeyValueOperation::Put && !isset($seenKeys[$key])) {
                $seenKeys[$key] = true;
                yield $key;
            }
        }
    }

    public function watch(string $keys = '>', WatchOptions ...$opts): KeyWatcher
    {
        return new KeyWatcher($this, $keys, array_values($opts));
    }

    /**
     * @param list<string> $keys
     */
    public function watchFiltered(array $keys, WatchOptions ...$opts): KeyWatcher
    {
        return new KeyWatcher($this, '>', array_values($opts), filterSubjects: $keys);
    }

    public function watchAll(WatchOptions ...$opts): KeyWatcher
    {
        return $this->watch('>', ...$opts);
    }

    /**
     * @return list<KeyValueEntry>
     */
    public function history(string $key): array
    {
        $this->validateKey($key);
        $entries = [];

        $kvSubject = "\$KV.{$this->bucketName}.{$key}";
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $streamInfo = $stream->info();

        for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
            try {
                $rawMsg = $stream->getMessage($seq);
                if ($rawMsg->subject === $kvSubject) {
                    $operation = KeyValueOperation::Put;
                    if ($rawMsg->headers !== null) {
                        $kvOp = $rawMsg->headers->get('KV-Operation');
                        if ($kvOp !== null) {
                            $operation = KeyValueOperation::from($kvOp);
                        }
                    }
                    $entries[] = new KeyValueEntry(
                        key: $key,
                        value: $rawMsg->data,
                        revision: $rawMsg->sequence,
                        delta: 0,
                        operation: $operation,
                        timestamp: $rawMsg->time ?? new \DateTimeImmutable(),
                        bucket: $this->bucketName,
                    );
                }
            } catch (\Throwable) {
                continue;
            }
        }

        return $entries;
    }

    public function status(): KeyValueStatus
    {
        $stream = $this->js->stream("KV_{$this->bucketName}");
        $info = $stream->info();

        $config = new KeyValueConfig(
            bucket: $this->bucketName,
            description: $info->config->description,
            maxBytes: $info->config->maxBytes,
            history: $info->config->maxMessagesPerSubject ?? 1,
            ttl: $info->config->maxAge !== null ? (float) $info->config->maxAge : null,
            maxValueSize: $info->config->maxMsgSize,
            replicas: $info->config->replicas,
            metadata: $info->config->metadata,
            mirror: $info->config->mirror,
            sources: $info->config->sources,
            compression: $info->config->compression !== \Nats\Enum\StoreCompression::None,
            rePublish: $info->config->rePublish,
        );

        return new KeyValueStatus(
            bucket: $this->bucketName,
            values: $info->state->messages,
            history: $info->config->maxMessagesPerSubject ?? 1,
            ttl: $info->config->maxAge,
            bytes: $info->state->bytes,
            backingStore: $info->config->storage->value,
            isCompressed: $info->config->compression !== \Nats\Enum\StoreCompression::None,
            config: $config,
            metadata: $info->config->metadata,
            streamInfo: $info,
        );
    }

    public function bucket(): string
    {
        return $this->bucketName;
    }

    /** @internal */
    public function jetStream(): JetStreamContext
    {
        return $this->js;
    }

    /** @internal */
    public function entryFromJetStreamMsg(JetStreamMessage $msg): KeyValueEntry
    {
        $subject = $msg->subject();
        $key = substr($subject, strlen("\$KV.{$this->bucketName}."));

        $operation = KeyValueOperation::Put;
        $headers = $msg->headers();
        if ($headers !== null) {
            $kvOp = $headers->get('KV-Operation');
            if ($kvOp !== null) {
                $operation = KeyValueOperation::from($kvOp);
            }
        }

        $meta = $msg->metadata();

        return new KeyValueEntry(
            key: $key,
            value: $msg->data(),
            revision: $meta->streamSequence,
            delta: $meta->numPending,
            operation: $operation,
            timestamp: $meta->timestamp,
            bucket: $this->bucketName,
        );
    }

    private function validateKey(string $key): void
    {
        if ($key === '' || str_contains($key, ' ') || str_starts_with($key, '.') || str_ends_with($key, '.')) {
            throw new NatsException("Invalid KV key: {$key}");
        }
    }

    /**
     * Match a NATS subject against a filter pattern supporting `*` and `>` wildcards.
     */
    private function subjectMatchesFilter(string $subject, string $filter): bool
    {
        if ($subject === $filter) {
            return true;
        }

        $subjectTokens = explode('.', $subject);
        $filterTokens = explode('.', $filter);

        $si = 0;
        $fi = 0;
        while ($si < count($subjectTokens) && $fi < count($filterTokens)) {
            $ft = $filterTokens[$fi];
            if ($ft === '>') {
                return true; // matches rest
            }
            if ($ft !== '*' && $ft !== $subjectTokens[$si]) {
                return false;
            }
            $si++;
            $fi++;
        }

        return $si === count($subjectTokens) && $fi === count($filterTokens);
    }
}
