<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Enum\DeliveryPolicy;
use Nats\Internal\TypeCast as T;
use Nats\Enum\StoreCompression;
use Nats\Headers;
use Nats\Inbox;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Message\JetStreamMsg;
use Nats\JetStream\Stream\StreamConfig;
use Nats\KeyValue\WatchOptions;
use Nats\Message;
use Nats\NatsException;

final class ObjectStore implements ObjectStoreInterface
{
    private int $chunkSize;

    /** @internal */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $bucketName,
        int $chunkSize = 131072, // 128KB
    ) {
        $this->chunkSize = self::requirePositiveChunkSize($chunkSize);
    }

    /**
     * @throws NatsException If the operation fails
     */
    public function put(ObjectMeta $meta, mixed $data): ObjectInfo
    {
        $content = is_resource($data) ? stream_get_contents($data) : (string) $data;
        $chunkSize = $meta->chunkSize ?? $this->chunkSize;
        return $this->putRaw($meta->name, $content, $meta->description, $meta->headers, $meta->link, $meta->metadata, $chunkSize);
    }

    public function putBytes(string $name, string $data): ObjectInfo
    {
        return $this->putRaw($name, $data);
    }

    public function putString(string $name, string $data): ObjectInfo
    {
        return $this->putRaw($name, $data);
    }

    /**
     * @throws NatsException If the file is not readable or the operation fails
     */
    public function putFile(string $filePath): ObjectInfo
    {
        if (!is_readable($filePath)) {
            throw new NatsException("File not readable: {$filePath}");
        }

        $handle = fopen($filePath, 'rb');
        if ($handle === false) {
            throw new NatsException("Failed to open file: {$filePath}");
        }

        try {
            return $this->putStream(basename($filePath), $handle);
        } finally {
            fclose($handle);
        }
    }

    /**
     * Upload data from a stream resource, reading in chunks to avoid loading the entire content into memory.
     *
     * @param string $name Object name
     * @param mixed $stream Readable stream resource
     * @throws NatsException If the stream is unreadable, the chunk size is invalid, or the upload fails
     */
    public function putStream(string $name, mixed $stream): ObjectInfo
    {
        // TODO: Refactor the shared upload flow in putStream() and putRaw() into a private helper to reduce drift.
        self::assertReadableStream($stream, $name);
        $chunkSize = self::requirePositiveChunkSize($this->chunkSize);
        $jsStream = $this->js->stream("OBJ_{$this->bucketName}");

        // Purge old chunks if overwriting
        try {
            $existing = $this->getObjectInfo($name);
            if (!$existing->deleted && $existing->nuid !== '') {
                $jsStream->purge(new \Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: "\$O.{$this->bucketName}.C.{$existing->nuid}",
                ));
            }
        } catch (\Throwable) {
            // Object doesn't exist yet
        }

        $nuid = Inbox::nuid();
        $chunkSubject = "\$O.{$this->bucketName}.C.{$nuid}";
        $hashCtx = hash_init('sha256');
        $totalSize = 0;
        $chunks = 0;

        try {
            while (!feof($stream)) {
                $chunk = fread($stream, $chunkSize);
                if ($chunk === false) {
                    throw new NatsException("Failed to read from stream for object: {$name}");
                }
                if ($chunk === '') {
                    if (feof($stream)) {
                        break;
                    }

                    throw new NatsException("Failed to read from stream for object: {$name}");
                }

                hash_update($hashCtx, $chunk);
                $this->js->publish($chunkSubject, $chunk);
                $totalSize += strlen($chunk);
                $chunks++;
            }

            if ($chunks === 0) {
                $this->js->publish($chunkSubject, '');
                $chunks = 1;
            }

            $digest = 'SHA-256=' . base64_encode(hash_final($hashCtx, true));

            $info = new ObjectInfo(
                name: $name,
                bucket: $this->bucketName,
                nuid: $nuid,
                size: $totalSize,
                chunks: $chunks,
                digest: $digest,
                deleted: false,
                mtime: new \DateTimeImmutable(),
            );

            $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
            $metaHeaders = new Headers();
            $metaHeaders->set('Nats-Rollup', 'sub');

            $msg = new Message(
                subject: $metaSubject,
                data: json_encode($info->toArray(), JSON_THROW_ON_ERROR),
                headers: $metaHeaders,
            );
            $this->js->publishMessage($msg);
        } catch (\Throwable $e) {
            try {
                $jsStream->purge(new \Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: $chunkSubject,
                ));
            } catch (\Throwable) {
                // Best effort cleanup for partial uploads.
            }

            throw $e;
        }

        return $info;
    }

    /**
     * @throws NatsException If the object is not found, deleted, or is a cross-bucket link
     */
    public function get(string $name): ObjectResult
    {
        $info = $this->getObjectInfo($name);
        if ($info->deleted) {
            throw new NatsException("Object deleted: {$name}");
        }

        // If this is a link to another object, resolve it
        if ($info->isLink() && $info->link !== null && $info->link->name !== '') {
            if ($info->link->bucket !== '' && $info->link->bucket !== $this->bucketName) {
                // Cross-bucket link - caller needs to resolve
                throw new NatsException("Object is a link to another bucket: {$info->link->bucket}/{$info->link->name}");
            }
            return $this->get($info->link->name);
        }

        // Fetch all chunks using direct stream message get
        $chunkSubject = "\$O.{$this->bucketName}.C.{$info->nuid}";
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $streamInfo = $stream->info();

        $data = '';
        for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
            try {
                $rawMsg = $stream->getMessage($seq);
                if ($rawMsg->subject === $chunkSubject) {
                    $data .= $rawMsg->data;
                }
            } catch (\Throwable) {
                continue;
            }
        }

        return new ObjectResult($info, $data);
    }

    public function getBytes(string $name): string
    {
        $result = $this->get($name);
        return $result->readAll();
    }

    public function getString(string $name): string
    {
        return $this->getBytes($name);
    }

    public function getFile(string $name, string $filePath): void
    {
        $data = $this->getBytes($name);
        $written = file_put_contents($filePath, $data);
        if ($written === false) {
            throw new NatsException("Failed to write to file: {$filePath}");
        }
    }

    public function getInfo(string $name, bool $showDeleted = false): ObjectInfo
    {
        $info = $this->getObjectInfo($name);
        if (!$showDeleted && $info->deleted) {
            throw new NatsException("Object not found: {$name}");
        }
        return $info;
    }

    /**
     * @throws NatsException If the object is not found or the operation fails
     */
    public function delete(string $name): void
    {
        $info = $this->getObjectInfo($name);

        // Purge chunks
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $stream->purge(new \Nats\JetStream\Stream\StreamPurgeOptions(
            filter: "\$O.{$this->bucketName}.C.{$info->nuid}",
        ));

        // Write delete marker to meta
        $deletedInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: $info->nuid,
            size: 0,
            chunks: 0,
            digest: '',
            deleted: true,
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($deletedInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);
    }

    public function updateMeta(string $name, ObjectMeta $meta): ObjectInfo
    {
        $existing = $this->getObjectInfo($name);

        $updatedInfo = new ObjectInfo(
            name: $meta->name,
            bucket: $this->bucketName,
            nuid: $existing->nuid,
            size: $existing->size,
            chunks: $existing->chunks,
            digest: $existing->digest,
            deleted: false,
            mtime: new \DateTimeImmutable(),
            description: $meta->description,
            headers: $meta->headers,
            link: $meta->link ?? $existing->link,
            metadata: $meta->metadata !== [] ? $meta->metadata : $existing->metadata,
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($meta->name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($updatedInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $updatedInfo;
    }

    public function addLink(string $name, ObjectInfo $target): ObjectInfo
    {
        if ($target->deleted) {
            throw new NatsException("Cannot link to a deleted object");
        }

        if ($target->isLink()) {
            throw new NatsException("Cannot link to another link");
        }

        $linkInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: '',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            mtime: new \DateTimeImmutable(),
            link: new ObjectLink(
                bucket: $target->bucket,
                name: $target->name,
            ),
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($linkInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $linkInfo;
    }

    public function addBucketLink(string $name, ObjectStoreInterface $targetStore): ObjectInfo
    {
        $linkInfo = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: '',
            size: 0,
            chunks: 0,
            digest: '',
            deleted: false,
            mtime: new \DateTimeImmutable(),
            link: new ObjectLink(
                bucket: $targetStore->bucket(),
            ),
        );

        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $headers = new Headers();
        $headers->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($linkInfo->toArray(), JSON_THROW_ON_ERROR),
            headers: $headers,
        );
        $this->js->publishMessage($msg);

        return $linkInfo;
    }

    /**
     * List objects in the store.
     *
     * @param bool $showDeleted Include deleted objects (inverse of Go's IgnoreDeletes)
     * @param bool $includeHistory Include all versions, not just latest (matches Go's IncludeHistory)
     * @return list<ObjectInfo>
     */
    public function list(bool $showDeleted = false, bool $includeHistory = false): array
    {
        $objects = [];
        $metaPrefix = "\$O.{$this->bucketName}.M.";
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $streamInfo = $stream->info();

        if ($includeHistory) {
            // Return all versions of all objects
            for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
                try {
                    $rawMsg = $stream->getMessage($seq);
                    if (str_starts_with($rawMsg->subject, $metaPrefix)) {
                        $info = ObjectInfo::fromArray(T::stringKeyArray(json_decode($rawMsg->data, true, 512, JSON_THROW_ON_ERROR)));
                        if ($showDeleted || !$info->deleted) {
                            $objects[] = $info;
                        }
                    }
                } catch (\Throwable) {
                    continue;
                }
            }
        } else {
            // Collect latest meta per object name by scanning stream
            $latestMeta = [];
            for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
                try {
                    $rawMsg = $stream->getMessage($seq);
                    if (str_starts_with($rawMsg->subject, $metaPrefix)) {
                        $info = ObjectInfo::fromArray(T::stringKeyArray(json_decode($rawMsg->data, true, 512, JSON_THROW_ON_ERROR)));
                        $latestMeta[$info->name] = $info;
                    }
                } catch (\Throwable) {
                    continue;
                }
            }

            foreach ($latestMeta as $info) {
                if ($showDeleted || !$info->deleted) {
                    $objects[] = $info;
                }
            }
        }

        return $objects;
    }

    public function watch(WatchOptions ...$opts): ObjectWatcher
    {
        return new ObjectWatcher($this->js, $this->bucketName, array_values($opts));
    }

    public function seal(): void
    {
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $currentInfo = $stream->info();

        // Create a new stream config with sealed=true
        $sealedConfig = new StreamConfig(
            name: "OBJ_{$this->bucketName}",
            subjects: $currentInfo->config->subjects,
            description: $currentInfo->config->description,
            retention: $currentInfo->config->retention,
            maxAge: $currentInfo->config->maxAge,
            maxBytes: $currentInfo->config->maxBytes,
            maxMsgSize: $currentInfo->config->maxMsgSize,
            maxMessages: $currentInfo->config->maxMessages,
            maxMessagesPerSubject: $currentInfo->config->maxMessagesPerSubject,
            storage: $currentInfo->config->storage,
            discard: $currentInfo->config->discard,
            replicas: $currentInfo->config->replicas,
            noAck: $currentInfo->config->noAck,
            denyDelete: $currentInfo->config->denyDelete,
            denyPurge: $currentInfo->config->denyPurge,
            allowRollup: $currentInfo->config->allowRollup,
            compression: $currentInfo->config->compression,
            placement: $currentInfo->config->placement,
            metadata: $currentInfo->config->metadata,
            sealed: true,
            allowDirect: $currentInfo->config->allowDirect,
            discardNewPerSubject: $currentInfo->config->discardNewPerSubject,
        );

        $this->js->updateStream($sealedConfig);
    }

    public function status(): ObjectStoreStatus
    {
        $stream = $this->js->stream("OBJ_{$this->bucketName}");
        $info = $stream->info();

        return new ObjectStoreStatus(
            bucket: $this->bucketName,
            size: $info->state->bytes,
            objects: $info->state->messages,
            config: new ObjectStoreConfig(
                bucket: $this->bucketName,
                description: $info->config->description,
                maxBytes: $info->config->maxBytes,
                storage: $info->config->storage,
                replicas: $info->config->replicas,
                metadata: $info->config->metadata,
                compression: $info->config->compression !== StoreCompression::None,
            ),
            backingStore: $info->config->storage->value,
            metadata: $info->config->metadata,
            sealed: $info->config->sealed,
            isCompressed: $info->config->compression !== StoreCompression::None,
            streamInfo: $info,
        );
    }

    public function bucket(): string
    {
        return $this->bucketName;
    }

    // --- Private ---

    /**
     * @param array<string, string> $metadata
     */
    private function putRaw(
        string $name,
        string $data,
        ?string $description = null,
        ?Headers $headers = null,
        ?ObjectLink $link = null,
        array $metadata = [],
        ?int $chunkSize = null,
    ): ObjectInfo {
        $chunkSize = self::requirePositiveChunkSize($chunkSize ?? $this->chunkSize);

        // Purge old chunks if overwriting
        try {
            $existing = $this->getObjectInfo($name);
            if (!$existing->deleted && $existing->nuid !== '') {
                $stream = $this->js->stream("OBJ_{$this->bucketName}");
                $stream->purge(new \Nats\JetStream\Stream\StreamPurgeOptions(
                    filter: "\$O.{$this->bucketName}.C.{$existing->nuid}",
                ));
            }
        } catch (\Throwable) {
            // Object doesn't exist yet - that's fine
        }

        $nuid = Inbox::nuid();
        $totalSize = strlen($data);
        $chunks = 0;

        // Compute digest
        $digest = 'SHA-256=' . base64_encode(hash('sha256', $data, true));

        // Publish chunks
        $chunkSubject = "\$O.{$this->bucketName}.C.{$nuid}";
        $offset = 0;
        while ($offset < $totalSize) {
            $chunk = substr($data, $offset, $chunkSize);
            $this->js->publish($chunkSubject, $chunk);
            $offset += strlen($chunk);
            $chunks++;
        }

        // Handle empty data
        if ($totalSize === 0) {
            $this->js->publish($chunkSubject, '');
            $chunks = 1;
        }

        // Build object info
        $info = new ObjectInfo(
            name: $name,
            bucket: $this->bucketName,
            nuid: $nuid,
            size: $totalSize,
            chunks: $chunks,
            digest: $digest,
            deleted: false,
            mtime: new \DateTimeImmutable(),
            description: $description,
            headers: $headers,
            link: $link,
            metadata: $metadata,
        );

        // Publish metadata with rollup
        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $metaHeaders = new Headers();
        $metaHeaders->set('Nats-Rollup', 'sub');

        $msg = new Message(
            subject: $metaSubject,
            data: json_encode($info->toArray(), JSON_THROW_ON_ERROR),
            headers: $metaHeaders,
        );
        $this->js->publishMessage($msg);

        return $info;
    }

    private function getObjectInfo(string $name): ObjectInfo
    {
        $metaSubject = "\$O.{$this->bucketName}.M.{$this->encodeName($name)}";
        $stream = $this->js->stream("OBJ_{$this->bucketName}");

        try {
            $rawMsg = $stream->getLastMessageForSubject($metaSubject);
        } catch (\Throwable) {
            throw new NatsException("Object not found: {$name}");
        }

        return ObjectInfo::fromArray(T::stringKeyArray(json_decode($rawMsg->data, true, 512, JSON_THROW_ON_ERROR)));
    }

    private function encodeName(string $name): string
    {
        // Replace special chars that aren't valid in NATS subjects
        return str_replace(['/', ' '], ['_', '_'], $name);
    }

    /**
     * @return int<1, max>
     */
    private static function requirePositiveChunkSize(int $chunkSize): int
    {
        if ($chunkSize <= 0) {
            throw new NatsException('Chunk size must be greater than 0');
        }

        return $chunkSize;
    }

    /**
     * @phpstan-assert resource $stream
     */
    private static function assertReadableStream(mixed $stream, string $name): void
    {
        if (!is_resource($stream) || get_resource_type($stream) !== 'stream') {
            throw new NatsException("Stream for object {$name} must be a readable stream resource");
        }

        $mode = stream_get_meta_data($stream)['mode'];
        if (!str_contains($mode, 'r') && !str_contains($mode, '+')) {
            throw new NatsException("Stream for object {$name} must be a readable stream resource");
        }
    }
}
