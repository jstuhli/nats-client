<?php

declare(strict_types=1);

namespace Nats\KeyValue;

interface KeyValueInterface
{
    public function put(string $key, string $value): int;

    public function get(string $key): KeyValueEntry;

    public function getRevision(string $key, int $revision): KeyValueEntry;

    public function create(string $key, string $value, ?float $keyTtl = null): int;

    public function update(string $key, string $value, int $revision): int;

    public function delete(string $key, ?int $lastRevision = null): void;

    public function purge(string $key, ?float $ttl = null): void;

    public function purgeDeletes(?float $ttl = null): void;

    /** @return \Generator<string> */
    public function listKeys(): \Generator;

    /** @return \Generator<string> */
    public function listKeysFiltered(string ...$filters): \Generator;

    public function watch(string $keys = '>', WatchOptions ...$opts): KeyWatcher;

    /**
     * @param list<string> $keys
     */
    public function watchFiltered(array $keys, WatchOptions ...$opts): KeyWatcher;

    public function watchAll(WatchOptions ...$opts): KeyWatcher;

    /**
     * @return list<KeyValueEntry>
     */
    public function history(string $key): array;

    public function status(): KeyValueStatus;

    public function bucket(): string;
}
