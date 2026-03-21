<?php

declare(strict_types=1);

namespace Nats\ObjectStore;

use Nats\Internal\TypeCast as T;
use Nats\JetStream\JetStreamContext;
use Nats\KeyValue\WatchOptions;

/**
 * @implements \IteratorAggregate<int, ObjectInfo>
 */
final class ObjectWatcher implements \IteratorAggregate
{
    private bool $stopped = false;

    /**
     * @internal
     * @param list<WatchOptions> $options
     */
    public function __construct(
        private readonly JetStreamContext $js,
        private readonly string $bucketName,
        private readonly array $options = [],
    ) {}

    /** @return \Generator<ObjectInfo> */
    public function updates(): \Generator
    {
        $updatesOnly = false;
        $ignoreDeletes = false;
        foreach ($this->options as $opt) {
            if ($opt->type === 'updates_only') { $updatesOnly = true; }
            if ($opt->type === 'ignore_deletes') { $ignoreDeletes = true; }
        }

        $metaPrefix = "\$O.{$this->bucketName}.M.";
        $stream = $this->js->stream("OBJ_{$this->bucketName}");

        $lastSeq = 0;

        // Initial scan of existing messages
        if (!$updatesOnly) {
            $streamInfo = $stream->info();
            for ($seq = $streamInfo->state->firstSeq; $seq <= $streamInfo->state->lastSeq; $seq++) {
                if ($this->stopped) {
                    return;
                }
                try {
                    $rawMsg = $stream->getMessage($seq);
                    if (str_starts_with($rawMsg->subject, $metaPrefix)) {
                        $info = ObjectInfo::fromArray(
                            T::stringKeyArray(json_decode($rawMsg->data, true, 512, JSON_THROW_ON_ERROR)),
                        );
                        if ($ignoreDeletes && $info->deleted) {
                            continue;
                        }
                        $lastSeq = $seq;
                        yield $info;
                    }
                } catch (\Throwable) {
                    continue;
                }
            }
        } else {
            $streamInfo = $stream->info();
            $lastSeq = $streamInfo->state->lastSeq;
        }

        // Poll for new messages
        while (!$this->stopped) {
            try {
                $streamInfo = $stream->info();
                $currentLast = $streamInfo->state->lastSeq;
                if ($currentLast > $lastSeq) {
                    for ($seq = $lastSeq + 1; $seq <= $currentLast; $seq++) {
                        try {
                            $rawMsg = $stream->getMessage($seq);
                            if (str_starts_with($rawMsg->subject, $metaPrefix)) {
                                $info = ObjectInfo::fromArray(
                                    T::stringKeyArray(json_decode($rawMsg->data, true, 512, JSON_THROW_ON_ERROR)),
                                );
                                if ($ignoreDeletes && $info->deleted) {
                                    continue;
                                }
                                yield $info;
                            }
                        } catch (\Throwable) {
                            continue;
                        }
                    }
                    $lastSeq = $currentLast;
                }
                usleep(100_000); // 100ms poll interval
            } catch (\Throwable) {
                break;
            }
        }
    }

    public function stop(): void
    {
        $this->stopped = true;
    }

    public function isClosed(): bool
    {
        return $this->stopped;
    }

    /** @return \Generator<ObjectInfo> */
    public function getIterator(): \Generator
    {
        return $this->updates();
    }
}
