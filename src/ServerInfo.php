<?php

declare(strict_types=1);

namespace Nats;

use Nats\Internal\TypeCast as T;

readonly class ServerInfo
{
    public function __construct(
        public string $serverId,
        public string $serverName,
        public string $version,
        public string $go,
        public string $host,
        public int $port,
        public bool $headersSupported,
        public bool $authRequired,
        public bool $tlsRequired,
        public bool $tlsAvailable,
        public int $maxPayload,
        public int $proto,
        public ?string $clientId = null,
        public ?string $clientIp = null,
        public ?string $nonce = null,
        public ?string $cluster = null,
        /** @var list<string> */
        public array $connectUrls = [],
        public bool $lameDuckMode = false,
        public bool $jetStream = false,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            serverId: T::string($data['server_id'] ?? ''),
            serverName: T::string($data['server_name'] ?? ''),
            version: T::string($data['version'] ?? ''),
            go: T::string($data['go'] ?? ''),
            host: T::string($data['host'] ?? ''),
            port: T::int($data['port'] ?? 0),
            headersSupported: T::bool($data['headers'] ?? false),
            authRequired: T::bool($data['auth_required'] ?? false),
            tlsRequired: T::bool($data['tls_required'] ?? false),
            tlsAvailable: T::bool($data['tls_available'] ?? false),
            maxPayload: T::int($data['max_payload'] ?? 0),
            proto: T::int($data['proto'] ?? 0),
            clientId: T::nullableString($data['client_id'] ?? null),
            clientIp: T::nullableString($data['client_ip'] ?? null),
            nonce: T::nullableString($data['nonce'] ?? null),
            cluster: T::nullableString($data['cluster'] ?? null),
            connectUrls: array_map(
                static fn(mixed $v): string => T::string($v),
                T::list($data['connect_urls'] ?? []),
            ),
            lameDuckMode: T::bool($data['ldm'] ?? false),
            jetStream: T::bool($data['jetstream'] ?? false),
        );
    }

    public static function fromJson(string $json): self
    {
        return self::fromArray(T::stringKeyArray(json_decode($json, true, 512, JSON_THROW_ON_ERROR)));
    }
}
