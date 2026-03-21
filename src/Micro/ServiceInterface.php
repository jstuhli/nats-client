<?php

declare(strict_types=1);

namespace Nats\Micro;

interface ServiceInterface
{
    public function addEndpoint(
        string $name,
        \Closure|HandlerInterface $handler,
        ?EndpointConfig $config = null,
    ): self;

    public function addGroup(string $name, ?string $queueGroup = null, bool $queueGroupDisabled = false): GroupInterface;

    public function info(): ServiceInfo;

    public function stats(): ServiceStats;

    public function reset(): void;

    public function stop(): void;

    public function isStopped(): bool;

    public function id(): string;

    public function name(): string;

    public function version(): string;
}
