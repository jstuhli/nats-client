<?php

declare(strict_types=1);

namespace Nats\JetStream\Publish;

use Nats\Internal\TypeCast as T;

final class PublishOptions
{
    private function __construct(
        public readonly string $type,
        public readonly mixed $value,
    ) {}

    public static function msgId(string $id): self
    {
        return new self('msg_id', $id);
    }

    public static function msgTtl(float $seconds): self
    {
        return new self('msg_ttl', $seconds);
    }

    public static function expectStream(string $stream): self
    {
        return new self('expect_stream', $stream);
    }

    public static function expectLastSequence(int $seq): self
    {
        return new self('expect_last_sequence', $seq);
    }

    public static function expectLastSequencePerSubject(int $seq): self
    {
        return new self('expect_last_sequence_per_subject', $seq);
    }

    public static function expectLastMsgId(string $id): self
    {
        return new self('expect_last_msg_id', $id);
    }

    public static function expectLastSequenceForSubject(int $seq, string $subject): self
    {
        return new self('expect_last_sequence_for_subject', ['seq' => $seq, 'subject' => $subject]);
    }

    public static function stallWait(float $seconds): self
    {
        return new self('stall_wait', $seconds);
    }

    public static function retryAttempts(int $num): self
    {
        return new self('retry_attempts', $num);
    }

    public static function retryWait(float $seconds): self
    {
        return new self('retry_wait', $seconds);
    }

    /**
     * Apply options to headers.
     *
     * @param list<PublishOptions> $options
     */
    public static function applyToHeaders(\Nats\Headers $headers, array $options): void
    {
        foreach ($options as $opt) {
            match ($opt->type) {
                'msg_id' => $headers->set('Nats-Msg-Id', T::string($opt->value)),
                'msg_ttl' => $headers->set('Nats-Msg-Ttl', (string) ((int) (T::float($opt->value) * 1_000_000_000))),
                'expect_stream' => $headers->set('Nats-Expected-Stream', T::string($opt->value)),
                'expect_last_sequence' => $headers->set('Nats-Expected-Last-Sequence', T::string($opt->value)),
                'expect_last_sequence_per_subject' => $headers->set('Nats-Expected-Last-Subject-Sequence', T::string($opt->value)),
                'expect_last_msg_id' => $headers->set('Nats-Expected-Last-Msg-Id', T::string($opt->value)),
                'expect_last_sequence_for_subject' => (static function () use ($headers, $opt): void {
                    $valueArr = T::stringKeyArray($opt->value);
                    $headers->set('Nats-Expected-Last-Subject-Sequence', T::string($valueArr['seq'] ?? 0));
                })(),
                default => null,
            };
        }
    }
}
