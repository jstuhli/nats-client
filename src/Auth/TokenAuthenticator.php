<?php

declare(strict_types=1);

namespace Nats\Auth;

readonly class TokenAuthenticator implements AuthenticatorInterface
{
    public function __construct(
        private string|\Closure $token,
    ) {}

    public function buildConnectOptions(): array
    {
        $token = $this->token instanceof \Closure ? ($this->token)() : $this->token;
        return ['auth_token' => $token];
    }

    public function sign(string $nonce): ?string
    {
        return null;
    }
}
