<?php

declare(strict_types=1);

namespace Nats;

use Nats\Auth\AuthenticatorInterface;
use Nats\Auth\CredentialsAuthenticator;
use Nats\Auth\JwtAuthenticator;
use Nats\Auth\NKeyAuthenticator;
use Nats\Auth\TokenAuthenticator;
use Nats\Auth\UserPassAuthenticator;

final readonly class ConnectionOptions
{
    /**
     * @param list<string> $servers
     * @param list<string> $tlsCaFiles
     * @param array<string, mixed> $tlsContextOptions
     * @param \Psr\Log\LoggerInterface|null $logger
     */
    public function __construct(
        /** @var list<string> */
        public array $servers = ['nats://127.0.0.1:4222'],
        public ?string $name = null,
        public ?AuthenticatorInterface $authenticator = null,

        // TLS
        public bool $tlsEnabled = false,
        public ?string $tlsCertFile = null,
        public ?string $tlsKeyFile = null,
        /** @var list<string> */
        public array $tlsCaFiles = [],
        /** @var array<string, mixed> */
        public array $tlsContextOptions = [],

        // Reconnect
        public int $maxReconnects = 60,
        public float $reconnectWait = 2.0,
        public float $reconnectJitter = 0.1,
        public float $reconnectJitterTls = 1.0,
        public int $reconnectBufferSize = 8 * 1024 * 1024,
        public bool $noReconnect = false,
        public bool $retryOnFailedConnect = false,
        public bool $dontRandomize = false,

        // Timeouts
        public float $timeout = 2.0,
        public float $pingInterval = 120.0,
        public int $maxPingsOutstanding = 2,
        public float $drainTimeout = 30.0,
        public float $flusherTimeout = 5.0,

        // Event handlers
        public ?\Closure $onConnect = null,
        public ?\Closure $onDisconnect = null,
        public ?\Closure $onReconnect = null,
        public ?\Closure $onClose = null,
        public ?\Closure $onError = null,
        public ?\Closure $onLameDuckMode = null,
        public ?\Closure $onDiscoveredServers = null,

        // Advanced
        public bool $noEcho = false,
        public bool $verbose = false,
        public bool $pedantic = false,
        public bool $ignoreDiscoveredServers = false,
        public string $inboxPrefix = '_INBOX',
        public bool $compression = false,

        // Additional options
        public bool $noCallbacksAfterClientClose = false,
        public bool $skipHostLookup = false,
        public bool $skipSubjectValidation = false,
        public ?\Closure $customReconnectDelay = null,
        public int $syncQueueLen = 65536,
        public bool $permissionErrOnSubscribe = false,

        // Logging
        public ?object $logger = null,
    ) {}

    // --- Withers ---

    /** @param list<string> $urls */
    public function withServers(string ...$urls): self
    {
        return clone($this, ['servers' => array_values($urls)]);
    }

    public function withName(string $name): self
    {
        return clone($this, ['name' => $name]);
    }

    // Auth

    public function withUserInfo(string $user, string $password): self
    {
        return clone($this, ['authenticator' => new UserPassAuthenticator($user, $password)]);
    }

    public function withToken(string|\Closure $token): self
    {
        return clone($this, ['authenticator' => new TokenAuthenticator($token)]);
    }

    public function withNkey(string $seed): self
    {
        return clone($this, ['authenticator' => new NKeyAuthenticator($seed)]);
    }

    public function withCredentials(string $credsFilePath): self
    {
        return clone($this, ['authenticator' => new CredentialsAuthenticator($credsFilePath)]);
    }

    public function withJwt(string $jwt, string $nkeySeed): self
    {
        return clone($this, ['authenticator' => new JwtAuthenticator($jwt, $nkeySeed)]);
    }

    public function withAuthenticator(AuthenticatorInterface $auth): self
    {
        return clone($this, ['authenticator' => $auth]);
    }

    // TLS

    public function withTls(bool $enabled = true): self
    {
        return clone($this, ['tlsEnabled' => $enabled]);
    }

    public function withTlsCertificate(string $certFile, string $keyFile): self
    {
        return clone($this, [
            'tlsCertFile' => $certFile,
            'tlsKeyFile' => $keyFile,
            'tlsEnabled' => true,
        ]);
    }

    public function withTlsCaCertificate(string ...$caFiles): self
    {
        return clone($this, [
            'tlsCaFiles' => array_values($caFiles),
            'tlsEnabled' => true,
        ]);
    }

    /** @param array<string, mixed> $streamContextOptions */
    public function withTlsConfig(array $streamContextOptions): self
    {
        return clone($this, [
            'tlsContextOptions' => $streamContextOptions,
            'tlsEnabled' => true,
        ]);
    }

    // Reconnect

    public function withMaxReconnects(int $max): self
    {
        return clone($this, ['maxReconnects' => $max]);
    }

    public function withReconnectWait(float $seconds): self
    {
        return clone($this, ['reconnectWait' => $seconds]);
    }

    public function withReconnectJitter(float $jitter, float $jitterTls = 1.0): self
    {
        return clone($this, [
            'reconnectJitter' => $jitter,
            'reconnectJitterTls' => $jitterTls,
        ]);
    }

    public function withReconnectBufferSize(int $bytes): self
    {
        return clone($this, ['reconnectBufferSize' => $bytes]);
    }

    public function withNoReconnect(bool $noReconnect = true): self
    {
        return clone($this, ['noReconnect' => $noReconnect]);
    }

    public function withRetryOnFailedConnect(bool $retry = true): self
    {
        return clone($this, ['retryOnFailedConnect' => $retry]);
    }

    public function withDontRandomize(bool $dontRandomize = true): self
    {
        return clone($this, ['dontRandomize' => $dontRandomize]);
    }

    // Timeouts

    public function withTimeout(float $seconds): self
    {
        return clone($this, ['timeout' => $seconds]);
    }

    public function withPingInterval(float $seconds): self
    {
        return clone($this, ['pingInterval' => $seconds]);
    }

    public function withMaxPingsOutstanding(int $max): self
    {
        return clone($this, ['maxPingsOutstanding' => $max]);
    }

    public function withDrainTimeout(float $seconds): self
    {
        return clone($this, ['drainTimeout' => $seconds]);
    }

    public function withFlusherTimeout(float $seconds): self
    {
        return clone($this, ['flusherTimeout' => $seconds]);
    }

    // Event handlers

    public function withOnConnect(\Closure $handler): self
    {
        return clone($this, ['onConnect' => $handler]);
    }

    public function withOnDisconnect(\Closure $handler): self
    {
        return clone($this, ['onDisconnect' => $handler]);
    }

    public function withOnReconnect(\Closure $handler): self
    {
        return clone($this, ['onReconnect' => $handler]);
    }

    public function withOnClose(\Closure $handler): self
    {
        return clone($this, ['onClose' => $handler]);
    }

    public function withOnError(\Closure $handler): self
    {
        return clone($this, ['onError' => $handler]);
    }

    public function withOnLameDuckMode(\Closure $handler): self
    {
        return clone($this, ['onLameDuckMode' => $handler]);
    }

    public function withOnDiscoveredServers(\Closure $handler): self
    {
        return clone($this, ['onDiscoveredServers' => $handler]);
    }

    // Advanced

    public function withNoEcho(bool $noEcho = true): self
    {
        return clone($this, ['noEcho' => $noEcho]);
    }

    public function withVerbose(bool $verbose = true): self
    {
        return clone($this, ['verbose' => $verbose]);
    }

    public function withPedantic(bool $pedantic = true): self
    {
        return clone($this, ['pedantic' => $pedantic]);
    }

    public function withIgnoreDiscoveredServers(bool $ignore = true): self
    {
        return clone($this, ['ignoreDiscoveredServers' => $ignore]);
    }

    public function withInboxPrefix(string $prefix): self
    {
        return clone($this, ['inboxPrefix' => $prefix]);
    }

    public function withCompression(bool $enabled = true): self
    {
        return clone($this, ['compression' => $enabled]);
    }

    /** Sets a PSR-3 logger for connection lifecycle events. */
    public function withLogger(object $logger): self
    {
        $loggerInterface = \Psr\Log\LoggerInterface::class;
        if (!interface_exists($loggerInterface) || !is_subclass_of($logger, $loggerInterface)) {
            throw new \InvalidArgumentException(
                "Logger must implement {$loggerInterface} (install psr/log to use logger support)"
            );
        }

        return clone($this, ['logger' => $logger]);
    }

    public function withNoCallbacksAfterClientClose(bool $enabled = true): self
    {
        return clone($this, ['noCallbacksAfterClientClose' => $enabled]);
    }

    public function withSkipHostLookup(bool $skip = true): self
    {
        return clone($this, ['skipHostLookup' => $skip]);
    }

    public function withSkipSubjectValidation(bool $skip = true): self
    {
        return clone($this, ['skipSubjectValidation' => $skip]);
    }

    public function withCustomReconnectDelay(\Closure $cb): self
    {
        return clone($this, ['customReconnectDelay' => $cb]);
    }

    public function withSyncQueueLen(int $max): self
    {
        return clone($this, ['syncQueueLen' => $max]);
    }

    public function withPermissionErrOnSubscribe(bool $enabled = true): self
    {
        return clone($this, ['permissionErrOnSubscribe' => $enabled]);
    }

    // --- Computed ---

    /** @return array<string, mixed> */
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
}
