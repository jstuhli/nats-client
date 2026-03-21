<?php

declare(strict_types=1);

namespace Nats\Micro;

interface GroupInterface
{
    public function addEndpoint(
        string $name,
        \Closure|HandlerInterface $handler,
        ?EndpointConfig $config = null,
    ): self;

    public function addGroup(string $name, ?string $queueGroup = null, bool $queueGroupDisabled = false): GroupInterface;
}
