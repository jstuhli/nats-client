<?php

declare(strict_types=1);

namespace Nats\Auth;

readonly class UserPassAuthenticator implements AuthenticatorInterface
{
    public function __construct(
        private string $user,
        private string $password,
    ) {}

    public function buildConnectOptions(): array
    {
        return [
            'user' => $this->user,
            'pass' => $this->password,
        ];
    }

    public function sign(string $nonce): ?string
    {
        return null;
    }
}
