<?php

declare(strict_types=1);

namespace Nats\Micro;

final class Group implements GroupInterface
{
    /** @internal */
    public function __construct(
        private readonly Service $service,
        private readonly string $prefix,
        private readonly ?string $queueGroup = null,
        private readonly bool $queueGroupDisabled = false,
    ) {}

    public function addEndpoint(
        string $name,
        \Closure|HandlerInterface $handler,
        ?EndpointConfig $config = null,
    ): self {
        $config ??= new EndpointConfig();

        $subject = $config->subject ?? $name;
        $fullSubject = $this->prefix . '.' . $subject;

        // Inherit group queue group settings unless endpoint overrides
        $queueGroup = $config->queueGroup ?? $this->queueGroup;
        $queueGroupDisabled = $config->queueGroupDisabled || $this->queueGroupDisabled;

        $endpointConfig = new EndpointConfig(
            subject: $fullSubject,
            metadata: $config->metadata,
            queueGroup: $queueGroup,
            queueGroupDisabled: $queueGroupDisabled,
        );

        $this->service->addEndpoint($name, $handler, $endpointConfig);
        return $this;
    }

    public function addGroup(string $name, ?string $queueGroup = null, bool $queueGroupDisabled = false): GroupInterface
    {
        // Inherit from parent group if not explicitly set
        $qg = $queueGroup ?? $this->queueGroup;
        $qgDisabled = $queueGroupDisabled || $this->queueGroupDisabled;

        return new Group($this->service, $this->prefix . '.' . $name, $qg, $qgDisabled);
    }
}
