<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\KeyValue\WatchOptions;

interface ObjectStoreInterface
{
    /** @param resource|string $data */
    public function put(ObjectMeta $meta, mixed $data): ObjectInfo;

    public function putBytes(string $name, string $data): ObjectInfo;

    public function putString(string $name, string $data): ObjectInfo;

    public function putFile(string $filePath): ObjectInfo;

    /** @param resource $stream Readable stream resource */
    public function putStream(string $name, mixed $stream): ObjectInfo;

    public function get(string $name): ObjectResult;

    public function getBytes(string $name): string;

    public function getString(string $name): string;

    public function getFile(string $name, string $filePath): void;

    public function getInfo(string $name, bool $showDeleted = false): ObjectInfo;

    public function delete(string $name): void;

    public function updateMeta(string $name, ObjectMeta $meta): ObjectInfo;

    public function addLink(string $name, ObjectInfo $target): ObjectInfo;

    public function addBucketLink(string $name, ObjectStoreInterface $targetStore): ObjectInfo;

    /** @return list<ObjectInfo> */
    public function list(bool $showDeleted = false): array;

    public function watch(WatchOptions ...$opts): ObjectWatcher;

    public function seal(): void;

    public function status(): ObjectStoreStatus;

    public function bucket(): string;
}
