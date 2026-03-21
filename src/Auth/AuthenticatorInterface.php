<?php

declare(strict_types=1);

namespace Nats\Auth;

interface AuthenticatorInterface
{
    /**
     * Return fields to merge into the CONNECT JSON payload.
     *
     * @return array<string, mixed>
     */
    public function buildConnectOptions(): array;

    /**
     * Sign a server nonce (for NKey/JWT auth).
     * Returns the base64-encoded signature, or null if not applicable.
     */
    public function sign(string $nonce): ?string;
}
