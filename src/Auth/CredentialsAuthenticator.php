<?php

declare(strict_types=1);

namespace Nats\Auth;

use Nats\NatsException;

readonly class CredentialsAuthenticator implements AuthenticatorInterface
{
    private string $jwt;
    private NKeyAuthenticator $nkeyAuth;

    public function __construct(string $credsFilePath)
    {
        if (!is_readable($credsFilePath)) {
            throw new NatsException("Credentials file not readable: {$credsFilePath}");
        }

        $content = file_get_contents($credsFilePath);
        if ($content === false) {
            throw new NatsException("Failed to read credentials file: {$credsFilePath}");
        }

        $this->jwt = self::extractBlock($content, 'NATS USER JWT');
        $seed = self::extractBlock($content, 'USER NKEY SEED');
        $this->nkeyAuth = new NKeyAuthenticator($seed);
    }

    public function buildConnectOptions(): array
    {
        return ['jwt' => $this->jwt];
    }

    public function sign(string $nonce): ?string
    {
        return $this->nkeyAuth->sign($nonce);
    }

    private static function extractBlock(string $content, string $label): string
    {
        $startMarker = "-----BEGIN {$label}-----";
        $endMarker = "------END {$label}------";

        $startPos = strpos($content, $startMarker);
        if ($startPos === false) {
            throw new NatsException("Missing block: {$label}");
        }

        $startPos += strlen($startMarker);
        $endPos = strpos($content, $endMarker, $startPos);
        if ($endPos === false) {
            throw new NatsException("Unterminated block: {$label}");
        }

        return trim(substr($content, $startPos, $endPos - $startPos));
    }
}
