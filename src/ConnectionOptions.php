<?php

declare(strict_types=1);

namespace Nats;

use Nats\Auth\AuthenticatorInterface;
use Nats\Auth\CredentialsAuthenticator;
use Nats\Auth\JwtAuthenticator;
use Nats\Auth\NKeyAuthenticator;
use Nats\Auth\TokenAuthenticator;
use Nats\Auth\UserPassAuthenticator;

final class ConnectionOptions
{
    /** @var list<string> */
    private array $servers = ['nats://127.0.0.1:4222'];
    private ?string $name = null;
    private ?AuthenticatorInterface $authenticator = null;

    // TLS
    private bool $tlsEnabled = false;
    private ?string $tlsCertFile = null;
    private ?string $tlsKeyFile = null;
    /** @var list<string> */
    private array $tlsCaFiles = [];
    /** @var array<string, mixed> */
    private array $tlsContextOptions = [];

    // Reconnect
    private int $maxReconnects = 60;
    private float $reconnectWait = 2.0;
    private float $reconnectJitter = 0.1;
    private float $reconnectJitterTls = 1.0;
    private int $reconnectBufferSize = 8 * 1024 * 1024; // 8MB
    private bool $noReconnect = false;
    private bool $retryOnFailedConnect = false;
    private bool $dontRandomize = false;

    // Timeouts
    private float $timeout = 2.0;
    private float $pingInterval = 120.0;
    private int $maxPingsOutstanding = 2;
    private float $drainTimeout = 30.0;
    private float $flusherTimeout = 5.0;

    // Event handlers
    private ?\Closure $onConnect = null;
    private ?\Closure $onDisconnect = null;
    private ?\Closure $onReconnect = null;
    private ?\Closure $onClose = null;
    private ?\Closure $onError = null;
    private ?\Closure $onLameDuckMode = null;
    private ?\Closure $onDiscoveredServers = null;

    // Advanced
    private bool $noEcho = false;
    private bool $verbose = false;
    private bool $pedantic = false;
    private bool $ignoreDiscoveredServers = false;
    private string $inboxPrefix = '_INBOX';
    private bool $compression = false;

    // Additional options
    private bool $noCallbacksAfterClientClose = false;
    private bool $skipHostLookup = false;
    private bool $skipSubjectValidation = false;
    private ?\Closure $customReconnectDelay = null;
    private int $syncQueueLen = 65536;
    private bool $permissionErrOnSubscribe = false;

    // Logging
    /** @var \Psr\Log\LoggerInterface|null */
    private ?object $logger = null;

    public function servers(string ...$urls): self
    {
        $this->servers = array_values($urls);
        return $this;
    }

    public function name(string $name): self
    {
        $this->name = $name;
        return $this;
    }

    // Auth methods

    public function userInfo(string $user, string $password): self
    {
        $this->authenticator = new UserPassAuthenticator($user, $password);
        return $this;
    }

    public function token(string|\Closure $token): self
    {
        $this->authenticator = new TokenAuthenticator($token);
        return $this;
    }

    public function nkey(string $seed): self
    {
        $this->authenticator = new NKeyAuthenticator($seed);
        return $this;
    }

    public function credentials(string $credsFilePath): self
    {
        $this->authenticator = new CredentialsAuthenticator($credsFilePath);
        return $this;
    }

    public function jwt(string $jwt, string $nkeySeed): self
    {
        $this->authenticator = new JwtAuthenticator($jwt, $nkeySeed);
        return $this;
    }

    public function authenticator(AuthenticatorInterface $auth): self
    {
        $this->authenticator = $auth;
        return $this;
    }

    // TLS

    public function tls(bool $enabled = true): self
    {
        $this->tlsEnabled = $enabled;
        return $this;
    }

    public function tlsCertificate(string $certFile, string $keyFile): self
    {
        $this->tlsCertFile = $certFile;
        $this->tlsKeyFile = $keyFile;
        $this->tlsEnabled = true;
        return $this;
    }

    public function tlsCaCertificate(string ...$caFiles): self
    {
        $this->tlsCaFiles = array_values($caFiles);
        $this->tlsEnabled = true;
        return $this;
    }

    /**
     * @param array<string, mixed> $streamContextOptions
     */
    public function tlsConfig(array $streamContextOptions): self
    {
        $this->tlsContextOptions = $streamContextOptions;
        $this->tlsEnabled = true;
        return $this;
    }

    // Reconnect

    public function maxReconnects(int $max): self
    {
        $this->maxReconnects = $max;
        return $this;
    }

    public function reconnectWait(float $seconds): self
    {
        $this->reconnectWait = $seconds;
        return $this;
    }

    public function reconnectJitter(float $jitter, float $jitterTls = 1.0): self
    {
        $this->reconnectJitter = $jitter;
        $this->reconnectJitterTls = $jitterTls;
        return $this;
    }

    public function reconnectBufferSize(int $bytes): self
    {
        $this->reconnectBufferSize = $bytes;
        return $this;
    }

    public function noReconnect(): self
    {
        $this->noReconnect = true;
        return $this;
    }

    public function retryOnFailedConnect(bool $retry = true): self
    {
        $this->retryOnFailedConnect = $retry;
        return $this;
    }

    public function dontRandomize(): self
    {
        $this->dontRandomize = true;
        return $this;
    }

    // Timeouts

    public function timeout(float $seconds): self
    {
        $this->timeout = $seconds;
        return $this;
    }

    public function pingInterval(float $seconds): self
    {
        $this->pingInterval = $seconds;
        return $this;
    }

    public function maxPingsOutstanding(int $max): self
    {
        $this->maxPingsOutstanding = $max;
        return $this;
    }

    public function drainTimeout(float $seconds): self
    {
        $this->drainTimeout = $seconds;
        return $this;
    }

    public function flusherTimeout(float $seconds): self
    {
        $this->flusherTimeout = $seconds;
        return $this;
    }

    // Event handlers

    public function onConnect(\Closure $handler): self
    {
        $this->onConnect = $handler;
        return $this;
    }

    public function onDisconnect(\Closure $handler): self
    {
        $this->onDisconnect = $handler;
        return $this;
    }

    public function onReconnect(\Closure $handler): self
    {
        $this->onReconnect = $handler;
        return $this;
    }

    public function onClose(\Closure $handler): self
    {
        $this->onClose = $handler;
        return $this;
    }

    public function onError(\Closure $handler): self
    {
        $this->onError = $handler;
        return $this;
    }

    public function onLameDuckMode(\Closure $handler): self
    {
        $this->onLameDuckMode = $handler;
        return $this;
    }

    public function onDiscoveredServers(\Closure $handler): self
    {
        $this->onDiscoveredServers = $handler;
        return $this;
    }

    // Advanced

    public function noEcho(): self
    {
        $this->noEcho = true;
        return $this;
    }

    public function verbose(bool $verbose = true): self
    {
        $this->verbose = $verbose;
        return $this;
    }

    public function pedantic(bool $pedantic = true): self
    {
        $this->pedantic = $pedantic;
        return $this;
    }

    public function ignoreDiscoveredServers(): self
    {
        $this->ignoreDiscoveredServers = true;
        return $this;
    }

    public function customInboxPrefix(string $prefix): self
    {
        $this->inboxPrefix = $prefix;
        return $this;
    }

    public function compression(bool $enabled = true): self
    {
        $this->compression = $enabled;
        return $this;
    }

    /**
     * @param \Psr\Log\LoggerInterface $logger
     */
    public function logger(object $logger): self
    {
        $this->logger = $logger;
        return $this;
    }

    public function noCallbacksAfterClientClose(): self
    {
        $this->noCallbacksAfterClientClose = true;
        return $this;
    }

    public function skipHostLookup(): self
    {
        $this->skipHostLookup = true;
        return $this;
    }

    public function skipSubjectValidation(): self
    {
        $this->skipSubjectValidation = true;
        return $this;
    }

    public function customReconnectDelay(\Closure $cb): self
    {
        $this->customReconnectDelay = $cb;
        return $this;
    }

    public function syncQueueLen(int $max): self
    {
        $this->syncQueueLen = $max;
        return $this;
    }

    public function permissionErrOnSubscribe(bool $enabled = true): self
    {
        $this->permissionErrOnSubscribe = $enabled;
        return $this;
    }

    // Dynamic setters (used by Connection at runtime)

    /** @internal */
    public function setOnDisconnect(?\Closure $handler): void
    {
        $this->onDisconnect = $handler;
    }

    /** @internal */
    public function setOnReconnect(?\Closure $handler): void
    {
        $this->onReconnect = $handler;
    }

    /** @internal */
    public function setOnClose(?\Closure $handler): void
    {
        $this->onClose = $handler;
    }

    /** @internal */
    public function setOnError(?\Closure $handler): void
    {
        $this->onError = $handler;
    }

    // Getters

    /** @return list<string> */
    public function getServers(): array
    {
        return $this->servers;
    }

    public function getName(): ?string
    {
        return $this->name;
    }

    public function getAuthenticator(): ?AuthenticatorInterface
    {
        return $this->authenticator;
    }

    public function isTlsEnabled(): bool
    {
        return $this->tlsEnabled;
    }

    /**
     * @return array<string, mixed>
     */
    public function getTlsContext(): array
    {
        $ctx = $this->tlsContextOptions;
        if ($this->tlsCertFile !== null) {
            $ctx['local_cert'] = $this->tlsCertFile;
        }
        if ($this->tlsKeyFile !== null) {
            $ctx['local_pk'] = $this->tlsKeyFile;
        }
        if ($this->tlsCaFiles !== []) {
            $ctx['cafile'] = $this->tlsCaFiles[0];
        }
        if (!isset($ctx['verify_peer'])) {
            $ctx['verify_peer'] = true;
        }
        if (!isset($ctx['verify_peer_name'])) {
            $ctx['verify_peer_name'] = true;
        }
        return $ctx;
    }

    public function getMaxReconnects(): int { return $this->maxReconnects; }
    public function getReconnectWait(): float { return $this->reconnectWait; }
    public function getReconnectJitter(): float { return $this->reconnectJitter; }
    public function getReconnectJitterTls(): float { return $this->reconnectJitterTls; }
    public function getReconnectBufferSize(): int { return $this->reconnectBufferSize; }
    public function isNoReconnect(): bool { return $this->noReconnect; }
    public function isRetryOnFailedConnect(): bool { return $this->retryOnFailedConnect; }
    public function isDontRandomize(): bool { return $this->dontRandomize; }
    public function getTimeout(): float { return $this->timeout; }
    public function getPingInterval(): float { return $this->pingInterval; }
    public function getMaxPingsOutstanding(): int { return $this->maxPingsOutstanding; }
    public function getDrainTimeout(): float { return $this->drainTimeout; }
    public function getFlusherTimeout(): float { return $this->flusherTimeout; }
    public function getOnConnect(): ?\Closure { return $this->onConnect; }
    public function getOnDisconnect(): ?\Closure { return $this->onDisconnect; }
    public function getOnReconnect(): ?\Closure { return $this->onReconnect; }
    public function getOnClose(): ?\Closure { return $this->onClose; }
    public function getOnError(): ?\Closure { return $this->onError; }
    public function getOnLameDuckMode(): ?\Closure { return $this->onLameDuckMode; }
    public function getOnDiscoveredServers(): ?\Closure { return $this->onDiscoveredServers; }
    public function isNoEcho(): bool { return $this->noEcho; }
    public function isVerbose(): bool { return $this->verbose; }
    public function isPedantic(): bool { return $this->pedantic; }
    public function isIgnoreDiscoveredServers(): bool { return $this->ignoreDiscoveredServers; }
    public function getInboxPrefix(): string { return $this->inboxPrefix; }
    public function isCompression(): bool { return $this->compression; }
    /** @return \Psr\Log\LoggerInterface|null */
    public function getLogger(): ?object { return $this->logger; }
    public function isNoCallbacksAfterClientClose(): bool { return $this->noCallbacksAfterClientClose; }
    public function isSkipHostLookup(): bool { return $this->skipHostLookup; }
    public function isSkipSubjectValidation(): bool { return $this->skipSubjectValidation; }
    public function getCustomReconnectDelay(): ?\Closure { return $this->customReconnectDelay; }
    public function getSyncQueueLen(): int { return $this->syncQueueLen; }
    public function isPermissionErrOnSubscribe(): bool { return $this->permissionErrOnSubscribe; }
}
