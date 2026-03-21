<?php

declare(strict_types=1);

namespace Nats\Micro;

interface HandlerInterface
{
    public function handle(RequestInterface $request): void;
}
