<?php

declare(strict_types=1);

namespace Nats\Auth;

readonly class JwtAuthenticator implements AuthenticatorInterface
{
    private NKeyAuthenticator $nkeyAuth;

    public function __construct(
        private string $jwt,
        string $nkeySeed,
    ) {
        $this->nkeyAuth = new NKeyAuthenticator($nkeySeed);
    }

    public function buildConnectOptions(): array
    {
        return ['jwt' => $this->jwt];
    }

    public function sign(string $nonce): ?string
    {
        return $this->nkeyAuth->sign($nonce);
    }
}
