<?php

declare(strict_types=1);

namespace Nats\KeyValue;

use Nats\Internal\TypeCast as T;

/**
 * @implements \IteratorAggregate<int, KeyValueEntry>
 */
final class KeyWatcher implements \IteratorAggregate
{
    private bool $stopped = false;

    /**
     * @internal
     * @param list<WatchOptions> $options
     * @param list<string> $filterSubjects
     */
    public function __construct(
        private readonly KeyValue $kv,
        private readonly string $keys,
        private readonly array $options,
        private readonly array $filterSubjects = [],
    ) {}

    /** @return \Generator<KeyValueEntry> */
    public function updates(): \Generator
    {
        // Parse options
        $updatesOnly = false;
        $ignoreDeletes = false;
        $includeHistory = false;
        $metaOnly = false;
        $resumeFromRevision = null;

        foreach ($this->options as $opt) {
            match ($opt->type) {
                'updates_only' => $updatesOnly = true,
                'ignore_deletes' => $ignoreDeletes = true,
                'include_history' => $includeHistory = true,
                'meta_only' => $metaOnly = true,
                'resume_from_revision' => $resumeFromRevision = T::int($opt->value),
                default => null,
            };
        }

        // Determine delivery policy based on options
        $deliverPolicy = \Nats\Enum\DeliveryPolicy::Last;
        $optStartSeq = null;

        if ($resumeFromRevision !== null) {
            $deliverPolicy = \Nats\Enum\DeliveryPolicy::ByStartSequence;
            $optStartSeq = $resumeFromRevision;
        } elseif ($includeHistory) {
            $deliverPolicy = \Nats\Enum\DeliveryPolicy::All;
        }

        if ($this->filterSubjects !== []) {
            $consumerConfig = new \Nats\JetStream\Consumer\ConsumerConfig(
                deliverPolicy: $deliverPolicy,
                optStartSeq: $optStartSeq,
                ackPolicy: \Nats\Enum\AckPolicy::None,
                filterSubjects: array_map(
                    fn(string $key) => "\$KV.{$this->kv->bucket()}.{$key}",
                    $this->filterSubjects,
                ),
                replayPolicy: \Nats\Enum\ReplayPolicy::Instant,
                memoryStorage: true,
                headersOnly: $metaOnly,
            );
        } else {
            $consumerConfig = new \Nats\JetStream\Consumer\ConsumerConfig(
                deliverPolicy: $deliverPolicy,
                optStartSeq: $optStartSeq,
                ackPolicy: \Nats\Enum\AckPolicy::None,
                filterSubject: "\$KV.{$this->kv->bucket()}.{$this->keys}",
                replayPolicy: \Nats\Enum\ReplayPolicy::Instant,
                memoryStorage: true,
                headersOnly: $metaOnly,
            );
        }

        $consumer = $this->kv->jetStream()->createConsumer(
            "KV_{$this->kv->bucket()}",
            $consumerConfig,
        );

        $messages = $consumer->messages(new \Nats\JetStream\Consumer\ConsumeOptions(
            maxMessages: 100,
            expires: 5.0,
        ));

        foreach ($messages as $msg) {
            if ($this->stopped) {
                $messages->stop();
                return;
            }

            $entry = $this->kv->entryFromJetStreamMsg($msg);

            if ($ignoreDeletes && $entry->operation !== \Nats\Enum\KeyValueOperation::Put) {
                continue;
            }

            yield $entry;
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

    /** @return \Generator<KeyValueEntry> */
    public function getIterator(): \Generator
    {
        return $this->updates();
    }
}
