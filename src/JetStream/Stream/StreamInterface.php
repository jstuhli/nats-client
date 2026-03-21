<?php

declare(strict_types=1);

namespace Nats\JetStream\Stream;

use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumerInterface;
use Nats\JetStream\Consumer\ConsumerPauseResponse;
use Nats\JetStream\Consumer\OrderedConsumerConfig;

interface StreamInterface
{
    public function createConsumer(ConsumerConfig $config): ConsumerInterface;

    public function createOrUpdateConsumer(ConsumerConfig $config): ConsumerInterface;

    public function updateConsumer(ConsumerConfig $config): ConsumerInterface;

    public function consumer(string $name): ConsumerInterface;

    public function orderedConsumer(?OrderedConsumerConfig $config = null): ConsumerInterface;

    public function deleteConsumer(string $name): void;

    public function pauseConsumer(string $name, \DateTimeImmutable $pauseUntil): ConsumerPauseResponse;

    public function resumeConsumer(string $name): ConsumerPauseResponse;

    /** @return \Generator<ConsumerConfig> */
    public function listConsumers(): \Generator;

    /** @return \Generator<string> */
    public function consumerNames(): \Generator;

    public function getMessage(int $sequence): RawStreamMessage;

    public function getLastMessageForSubject(string $subject): RawStreamMessage;

    public function deleteMessage(int $sequence): void;

    public function secureDeleteMessage(int $sequence): void;

    public function purge(?StreamPurgeOptions $options = null): int;

    public function info(bool $deletedDetails = false, ?string $subjectFilter = null): StreamInfo;

    public function cachedInfo(): StreamInfo;
}
