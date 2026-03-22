<?php

declare(strict_types=1);

namespace Nats;

use Nats\Enum\ConnectionStatus;
use Nats\Protocol\Command;
use Nats\Protocol\Parser;
use Nats\Protocol\ServerOp;
use Nats\Protocol\Writer;
use Nats\Transport\TcpTransport;
use Nats\Transport\TransportInterface;

final class Connection
{
    /** Default read buffer size per syscall — matches Go client defaultBufSize (32KB). */
    private const int DEFAULT_BUF_SIZE = 32768;

    /** Default reconnect buffer size — matches Go client DefaultReconnectBufSize (8MB). */
    public const int DEFAULT_RECONNECT_BUF_SIZE = 8_388_608;

    private ConnectionStatus $status = ConnectionStatus::Disconnected;
    private ?ServerInfo $serverInfo = null;
    private TransportInterface $transport;
    private Parser $parser;
    private Writer $writer;
    private ConnectionOptions $options;
    private string $writeBuffer = '';

    /** @var array<string, Subscription> */
    private array $subscriptions = [];
    private int $ssid = 0;

    /** @var array<string, \Closure> Pending pong callbacks */
    private array $pongCallbacks = [];
    private int $pongCounter = 0;
    private int $pingsOut = 0;
    private float $lastPingTime = 0;

    /** @var list<string> All known server URLs */
    private array $serverPool = [];
    private int $currentServerIndex = 0;

    // Stats
    private int $inMsgs = 0;
    private int $outMsgs = 0;
    private int $inBytes = 0;
    private int $outBytes = 0;
    private int $reconnects = 0;

    private ?\Throwable $lastError = null;

    /** @var \Psr\Log\LoggerInterface|null */
    private ?object $logger = null;

    // Dynamic event handlers (override options at runtime)
    private ?\Closure $onDisconnectHandler = null;
    private ?\Closure $onReconnectHandler = null;
    private ?\Closure $onCloseHandler = null;
    private ?\Closure $onErrorHandler = null;

    // Reconnect buffer
    private string $reconnectBuffer = '';

    private function __construct() {}

    /**
     * @param string|list<string> $url
     * @throws NatsException If unable to connect to any NATS server
     * @throws AuthorizationException If authentication fails
     */
    public static function connect(
        string|array $url = 'nats://127.0.0.1:4222',
        ?ConnectionOptions $options = null,
    ): self {
        $conn = new self();
        $conn->options = $options ?? new ConnectionOptions();
        $conn->logger = $conn->options->logger;
        $conn->onDisconnectHandler = $conn->options->onDisconnect;
        $conn->onReconnectHandler = $conn->options->onReconnect;
        $conn->onCloseHandler = $conn->options->onClose;
        $conn->onErrorHandler = $conn->options->onError;
        $conn->parser = new Parser();
        $conn->writer = new Writer();
        $conn->transport = new TcpTransport();

        // Build server pool
        if (is_string($url)) {
            $conn->serverPool = array_map('trim', explode(',', $url));
        } else {
            $conn->serverPool = $url;
        }

        // Override with options servers if set
        $optServers = $conn->options->servers;
        if ($optServers !== ['nats://127.0.0.1:4222'] || is_string($url) && $url === 'nats://127.0.0.1:4222') {
            if ($optServers !== ['nats://127.0.0.1:4222']) {
                $conn->serverPool = $optServers;
            }
        }

        // Resolve hostnames to IPs unless disabled
        if (!$conn->options->skipHostLookup) {
            $conn->resolveServerPool();
        }

        // Randomize server pool unless disabled
        if (!$conn->options->dontRandomize && count($conn->serverPool) > 1) {
            shuffle($conn->serverPool);
        }

        $conn->doConnect();

        return $conn;
    }

    // --- Publishing ---

    /**
     * @throws NatsException If not connected or subject is invalid
     * @throws MaxPayloadException If data exceeds server max payload
     */
    public function publish(string $subject, string $data = '', ?string $replyTo = null): void
    {
        $this->ensureConnected();
        self::validateSubject($subject);
        $this->checkMaxPayload(strlen($data));

        $cmd = $this->writer->pub($subject, $data, $replyTo);
        $this->writeToServer($cmd);
        $this->outMsgs++;
        $this->outBytes += strlen($data);
    }

    /**
     * @throws NatsException If not connected, subject is invalid, or headers not supported
     * @throws MaxPayloadException If message exceeds server max payload
     */
    public function publishMessage(Message $msg): void
    {
        $this->ensureConnected();
        self::validateSubject($msg->subject);

        if ($msg->headers !== null && $msg->headers->count() > 0) {
            if (!$this->headersSupported()) {
                throw new NatsException('Headers not supported by this server');
            }
            $headerWire = $msg->headers->toWireFormat();
            $this->checkMaxPayload(strlen($headerWire) + strlen($msg->data));
            $cmd = $this->writer->hpub($msg->subject, $headerWire, $msg->data, $msg->replyTo);
        } else {
            $this->checkMaxPayload(strlen($msg->data));
            $cmd = $this->writer->pub($msg->subject, $msg->data, $msg->replyTo);
        }

        $this->writeToServer($cmd);
        $this->outMsgs++;
        $this->outBytes += strlen($msg->data);
    }

    // --- Subscribing ---

    /**
     * @throws NatsException If not connected or subject is invalid
     */
    public function subscribe(string $subject, \Closure $handler): Subscription
    {
        return $this->doSubscribe($subject, null, $handler);
    }

    /**
     * @throws NatsException If not connected or subject is invalid
     */
    public function subscribeSync(string $subject): Subscription
    {
        return $this->doSubscribe($subject, null, null);
    }

    /**
     * @throws NatsException If not connected or subject is invalid
     */
    public function queueSubscribe(string $subject, string $queue, \Closure $handler): Subscription
    {
        return $this->doSubscribe($subject, $queue, $handler);
    }

    /**
     * @throws NatsException If not connected or subject is invalid
     */
    public function queueSubscribeSync(string $subject, string $queue): Subscription
    {
        return $this->doSubscribe($subject, $queue, null);
    }

    // --- Request-Reply ---

    /**
     * @throws NatsException If not connected, no responders, or subject is invalid
     * @throws TimeoutException If no response received within timeout
     */
    public function request(string $subject, string $data = '', float $timeout = 2.0): Message
    {
        $inbox = $this->newInbox();
        $sub = $this->subscribeSync($inbox);
        $sub->autoUnsubscribe(1);

        $this->publish($subject, $data, $inbox);
        $this->flushWrite();

        $msg = $sub->nextMessage($timeout);

        // Check for no responders (status 503)
        if ($msg->headers !== null) {
            $status = $msg->headers->get('Status');
            if ($status !== null && str_starts_with(trim($status), '503')) {
                throw new NatsException('No responders for subject: ' . $subject);
            }
        }

        return $msg;
    }

    /**
     * @throws NatsException If not connected or subject is invalid
     * @throws TimeoutException If no response received within timeout
     */
    public function requestMessage(Message $msg, float $timeout = 2.0): Message
    {
        $inbox = $this->newInbox();
        $sub = $this->subscribeSync($inbox);
        $sub->autoUnsubscribe(1);

        $request = new Message(
            subject: $msg->subject,
            data: $msg->data,
            replyTo: $inbox,
            headers: $msg->headers,
        );
        $this->publishMessage($request);
        $this->flushWrite();

        return $sub->nextMessage($timeout);
    }

    // --- Connection lifecycle ---

    public function close(): void
    {
        if ($this->status === ConnectionStatus::Closed) {
            return;
        }

        $this->status = ConnectionStatus::Closed;
        $this->log('info', 'Connection closed');
        $this->flushWrite();
        $this->transport->close();
        $this->parser->reset();
        $this->subscriptions = [];

        $handler = $this->onCloseHandler;
        if ($handler !== null) {
            $handler($this);
        }
    }

    public function drain(): void
    {
        $this->log('info', 'Draining connection');
        $this->status = ConnectionStatus::DrainingSubscriptions;

        // Unsubscribe all
        foreach ($this->subscriptions as $sub) {
            $sub->drain();
        }

        $this->status = ConnectionStatus::DrainingPublications;
        $this->flush();
        $this->close();
    }

    /**
     * @throws NatsException If not connected
     * @throws TimeoutException If flush does not complete within timeout
     */
    public function flush(float $timeout = 2.0): void
    {
        $this->ensureConnected();
        $this->flushWrite();

        // Send PING and wait for PONG
        $done = false;
        $id = 'p' . ++$this->pongCounter;
        $this->pongCallbacks[$id] = static function () use (&$done): void {
            $done = true;
        };

        $this->writeToServer($this->writer->ping());
        $this->flushWrite();

        $deadline = microtime(true) + $timeout;
        while (!$done && microtime(true) < $deadline) {
            $remaining = $deadline - microtime(true);
            $this->processIncoming(max(0.001, $remaining));
        }

        unset($this->pongCallbacks[$id]);

        if (!$done) {
            throw new TimeoutException('Flush timed out');
        }
    }

    public function forceReconnect(): void
    {
        $this->transport->close();
        $this->status = ConnectionStatus::Reconnecting;
        $this->doReconnect();
    }

    // --- Status ---

    public function isConnected(): bool
    {
        return $this->status === ConnectionStatus::Connected;
    }

    public function isClosed(): bool
    {
        return $this->status === ConnectionStatus::Closed;
    }

    public function isReconnecting(): bool
    {
        return $this->status === ConnectionStatus::Reconnecting;
    }

    public function isDraining(): bool
    {
        return $this->status === ConnectionStatus::DrainingSubscriptions
            || $this->status === ConnectionStatus::DrainingPublications;
    }

    public function status(): ConnectionStatus
    {
        return $this->status;
    }

    public function connectedUrl(): string
    {
        return $this->serverPool[$this->currentServerIndex] ?? '';
    }

    public function connectedServerName(): string
    {
        return $this->serverInfo !== null ? $this->serverInfo->serverName : '';
    }

    public function connectedServerId(): string
    {
        return $this->serverInfo !== null ? $this->serverInfo->serverId : '';
    }

    public function connectedClusterName(): string
    {
        return $this->serverInfo !== null ? ($this->serverInfo->cluster ?? '') : '';
    }

    public function connectedServerVersion(): string
    {
        return $this->serverInfo !== null ? $this->serverInfo->version : '';
    }

    /** @return list<string> */
    public function servers(): array
    {
        return $this->serverPool;
    }

    /** @return list<string> */
    public function discoveredServers(): array
    {
        return $this->serverInfo !== null ? $this->serverInfo->connectUrls : [];
    }

    public function maxPayload(): int
    {
        return $this->serverInfo !== null ? $this->serverInfo->maxPayload : 1_048_576;
    }

    public function headersSupported(): bool
    {
        return $this->serverInfo !== null ? $this->serverInfo->headersSupported : false;
    }

    public function jetStreamAvailable(): bool
    {
        return $this->serverInfo !== null ? $this->serverInfo->jetStream : false;
    }

    public function stats(): Statistics
    {
        return new Statistics(
            inMsgs: $this->inMsgs,
            outMsgs: $this->outMsgs,
            inBytes: $this->inBytes,
            outBytes: $this->outBytes,
            reconnects: $this->reconnects,
        );
    }

    public function serverInfo(): ?ServerInfo
    {
        return $this->serverInfo;
    }

    /**
     * @throws NatsException If not connected
     * @throws TimeoutException If RTT measurement times out
     */
    public function rtt(): float
    {
        $this->ensureConnected();

        $done = false;
        $id = 'p' . ++$this->pongCounter;
        $this->pongCallbacks[$id] = static function () use (&$done): void {
            $done = true;
        };

        $start = hrtime(true);
        $this->writeToServer($this->writer->ping());
        $this->flushWrite();

        $deadline = microtime(true) + 5.0;
        while (!$done && microtime(true) < $deadline) {
            $remaining = $deadline - microtime(true);
            $this->processIncoming(max(0.001, $remaining));
        }

        $elapsed = (hrtime(true) - $start) / 1_000_000_000;

        unset($this->pongCallbacks[$id]);

        if (!$done) {
            throw new TimeoutException('RTT measurement timed out');
        }

        return $elapsed;
    }

    public function connectedAddr(): string
    {
        $url = $this->serverPool[$this->currentServerIndex] ?? '';
        $parsed = parse_url($url);
        if ($parsed === false) {
            return '127.0.0.1:4222';
        }
        $host = $parsed['host'] ?? '127.0.0.1';
        $port = $parsed['port'] ?? 4222;
        return "{$host}:{$port}";
    }

    public function lastError(): ?\Throwable
    {
        return $this->lastError;
    }

    public function numSubscriptions(): int
    {
        return count($this->subscriptions);
    }

    /**
     * Returns the current size of the write buffer in bytes.
     */
    public function buffered(): int
    {
        return strlen($this->writeBuffer);
    }

    /**
     * Calls the given callback after all prior published messages have been flushed
     * and processed by the server (i.e., after receiving a PONG response).
     */
    public function barrier(\Closure $fn): void
    {
        $this->ensureConnected();
        $this->flushWrite();

        $id = 'p' . ++$this->pongCounter;
        $this->pongCallbacks[$id] = static function () use ($fn): void {
            $fn();
        };

        $this->writeToServer($this->writer->ping());
        $this->flushWrite();
    }

    /**
     * Dynamically set the disconnect handler on the connection.
     */
    public function setDisconnectHandler(?\Closure $handler): void
    {
        $this->onDisconnectHandler = $handler;
    }

    /**
     * Dynamically set the reconnect handler on the connection.
     */
    public function setReconnectHandler(?\Closure $handler): void
    {
        $this->onReconnectHandler = $handler;
    }

    /**
     * Dynamically set the closed handler on the connection.
     */
    public function setClosedHandler(?\Closure $handler): void
    {
        $this->onCloseHandler = $handler;
    }

    /**
     * Dynamically set the error handler on the connection.
     */
    public function setErrorHandler(?\Closure $handler): void
    {
        $this->onErrorHandler = $handler;
    }

    public function authRequired(): bool
    {
        return $this->serverInfo !== null ? $this->serverInfo->authRequired : false;
    }

    public function tlsRequired(): bool
    {
        return $this->serverInfo !== null ? $this->serverInfo->tlsRequired : false;
    }

    public function clientId(): ?string
    {
        return $this->serverInfo?->clientId;
    }

    public function clientIp(): ?string
    {
        return $this->serverInfo?->clientIp;
    }

    public function newInbox(): string
    {
        return Inbox::generate($this->options->inboxPrefix);
    }

    // --- JetStream ---

    /**
     * @throws NatsException If JetStream is not available on this server
     */
    public function jetStream(?JetStream\JetStreamOptions $options = null): JetStream\JetStreamContext
    {
        if (!$this->jetStreamAvailable()) {
            throw new NatsException('JetStream is not available on this server');
        }
        return new JetStream\JetStreamContext($this, $options ?? new JetStream\JetStreamOptions());
    }

    // --- Event Loop ---

    public function wait(): void
    {
        while ($this->status === ConnectionStatus::Connected) {
            $this->processIncoming(0.1);
            $this->checkPing();
        }
    }

    public function process(float $timeout = 0.0): void
    {
        $this->processIncoming($timeout);
    }

    // --- Internal: connect ---

    private function doConnect(): void
    {
        $this->status = ConnectionStatus::Connecting;
        $lastError = null;

        for ($i = 0; $i < count($this->serverPool); $i++) {
            $idx = ($this->currentServerIndex + $i) % count($this->serverPool);
            $url = $this->serverPool[$idx];

            try {
                $this->connectToServer($url);
                $this->currentServerIndex = $idx;
                $this->status = ConnectionStatus::Connected;

                $this->log('info', 'Connected to {url}', ['url' => $url]);

                $handler = $this->options->onConnect;
                if ($handler !== null) {
                    $handler($this);
                }

                return;
            } catch (\Throwable $e) {
                $this->log('warning', 'Failed to connect to {url}: {error}', [
                    'url' => $url,
                    'error' => $e->getMessage(),
                ]);
                $lastError = $e;
                continue;
            }
        }

        $this->status = ConnectionStatus::Disconnected;
        $errorMsg = $lastError?->getMessage() ?? 'unknown error';
        $this->log('error', 'Failed to connect to any NATS server: {error}', ['error' => $errorMsg]);
        throw new NatsException('Failed to connect to any NATS server: ' . $errorMsg);
    }

    private function connectToServer(string $url): void
    {
        $parsed = parse_url($url);
        if ($parsed === false) {
            throw new NatsException("Invalid server URL: {$url}");
        }
        $host = $parsed['host'] ?? '127.0.0.1';
        $port = $parsed['port'] ?? 4222;
        $scheme = $parsed['scheme'] ?? 'nats';

        $useTls = $scheme === 'tls' || $scheme === 'nats+tls' || $this->options->tlsEnabled;
        $address = "tcp://{$host}:{$port}";

        $this->log('debug', 'Attempting to connect to {url}', ['url' => $url]);

        $tlsContext = $useTls ? $this->options->getTlsContext() : null;

        $this->transport->connect($address, $this->options->timeout, $tlsContext);
        $this->transport->setTimeout($this->options->timeout);

        // Read INFO
        $infoLine = $this->transport->readLine();
        $infoLine = rtrim($infoLine, "\r\n");

        if (!str_starts_with($infoLine, 'INFO ')) {
            throw new NatsException("Expected INFO, got: {$infoLine}");
        }

        $serverInfo = ServerInfo::fromJson(substr($infoLine, 5));
        $this->serverInfo = $serverInfo;
        $this->parser->setMaxPayload($serverInfo->maxPayload);

        // TLS upgrade if server requires it but we connected plain
        if ($serverInfo->tlsRequired && !$useTls) {
            $this->log('debug', 'Upgrading to TLS');
            $this->transport->upgradeTls($this->options->getTlsContext());
        }

        // Add discovered servers to pool
        if (!$this->options->ignoreDiscoveredServers) {
            $newServers = 0;
            foreach ($serverInfo->connectUrls as $discoveredUrl) {
                $normalized = "nats://{$discoveredUrl}";
                if (!in_array($normalized, $this->serverPool, true)) {
                    $this->serverPool[] = $normalized;
                    $newServers++;
                }
            }
            if ($newServers > 0) {
                $this->log('debug', 'Discovered {count} new servers', ['count' => $newServers]);
            }
        }

        // Build CONNECT payload
        $connectPayload = $this->buildConnectPayload();
        $this->transport->write($this->writer->connect($connectPayload));
        $this->transport->write($this->writer->ping());

        // Wait for +OK or PONG (or -ERR)
        $response = $this->transport->readLine();
        $response = rtrim($response, "\r\n");

        if (str_starts_with($response, '-ERR')) {
            $errMsg = trim(substr($response, 4), " '\"");
            if (str_contains($errMsg, 'Authorization')) {
                throw new AuthorizationException($errMsg);
            }
            throw new NatsException("Server error: {$errMsg}");
        }

        // Might get +OK then PONG, or just PONG
        if ($response === '+OK') {
            $response = $this->transport->readLine();
        }

        $this->pingsOut = 0;
        $this->lastPingTime = microtime(true);

        // Set URL user/pass auth if present in URL
        if (isset($parsed['user']) && $this->options->authenticator === null) {
            // URL had embedded credentials — already handled in buildConnectPayload
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function buildConnectPayload(): array
    {
        $payload = [
            'verbose' => $this->options->verbose,
            'pedantic' => $this->options->pedantic,
            'lang' => 'php',
            'version' => '1.0.0',
            'protocol' => 1,
            'headers' => true,
            'no_responders' => true,
        ];

        if ($this->options->name !== null) {
            $payload['name'] = $this->options->name;
        }

        if ($this->options->noEcho) {
            $payload['echo'] = false;
        }

        // Auth
        $auth = $this->options->authenticator;
        if ($auth !== null) {
            $payload = array_merge($payload, $auth->buildConnectOptions());

            // Sign nonce if server provided one
            if ($this->serverInfo?->nonce !== null) {
                $sig = $auth->sign($this->serverInfo->nonce);
                if ($sig !== null) {
                    $payload['sig'] = $sig;
                }
            }
        }

        // URL-embedded credentials
        $url = $this->serverPool[$this->currentServerIndex] ?? '';
        $parsed = parse_url($url);
        if ($parsed !== false && isset($parsed['user']) && $auth === null) {
            $payload['user'] = $parsed['user'];
            if (isset($parsed['pass'])) {
                $payload['pass'] = $parsed['pass'];
            }
        }

        return $payload;
    }

    // --- Internal: subscribe ---

    private function doSubscribe(string $subject, ?string $queue, ?\Closure $handler): Subscription
    {
        $this->ensureConnected();
        self::validateSubject($subject);

        $sid = 's' . ++$this->ssid;
        $sub = new Subscription(
            connection: $this,
            sid: $sid,
            subject: $subject,
            queue: $queue,
            handler: $handler,
        );

        $this->subscriptions[$sid] = $sub;

        $cmd = $this->writer->sub($subject, $sid, $queue);
        $this->writeToServer($cmd);

        $this->log('debug', 'Subscribed to {subject} (sid={sid})', ['subject' => $subject, 'sid' => $sid]);

        return $sub;
    }

    /** @internal */
    public function unsubscribeSid(string $sid, ?int $maxMessages = null): void
    {
        $this->log('debug', 'Unsubscribed sid={sid}', ['sid' => $sid]);
        $cmd = $this->writer->unsub($sid, $maxMessages);
        $this->writeToServer($cmd);

        if ($maxMessages === null || $maxMessages === 0) {
            unset($this->subscriptions[$sid]);
        }
    }

    /** @internal */
    public function removeSubscription(string $sid): void
    {
        unset($this->subscriptions[$sid]);
    }

    /** @internal Report slow consumer to error handler (matches Go async error callback) */
    public function reportSlowConsumer(Subscription $sub): void
    {
        $this->log('warning', "Slow consumer on subject '{subject}'", ['subject' => $sub->subject()]);
        $this->lastError = new SlowConsumerException(
            "Slow consumer on subject '{$sub->subject()}': messages dropped"
        );

        $handler = $this->onErrorHandler;
        if ($handler !== null) {
            $handler($this, $this->lastError);
        }
    }

    // --- Internal: I/O ---

    private function writeToServer(string $data): void
    {
        if ($this->status === ConnectionStatus::Reconnecting) {
            $maxSize = $this->options->reconnectBufferSize;
            if (strlen($this->reconnectBuffer) + strlen($data) <= $maxSize) {
                $this->reconnectBuffer .= $data;
            }
            return;
        }

        $this->writeBuffer .= $data;
    }

    private function flushWrite(): void
    {
        if ($this->writeBuffer === '') {
            return;
        }
        if (!$this->transport->isConnected()) {
            return;
        }

        $this->transport->write($this->writeBuffer);
        $this->writeBuffer = '';
    }

    private function processIncoming(float $timeout): void
    {
        $this->flushWrite();

        if (!$this->transport->isConnected()) {
            if ($this->status === ConnectionStatus::Connected) {
                $this->handleDisconnect();
            }
            return;
        }

        if (!$this->transport->waitForData($timeout)) {
            return;
        }

        try {
            $data = $this->transport->read(self::DEFAULT_BUF_SIZE);
        } catch (\Throwable) {
            $this->handleDisconnect();
            return;
        }

        if ($data === '') {
            return;
        }

        $this->inBytes += strlen($data);
        $ops = $this->parser->parse($data);

        foreach ($ops as $op) {
            $this->handleServerOp($op);
        }
    }

    private function handleServerOp(ServerOp $op): void
    {
        match ($op->command) {
            Command::Msg, Command::HMsg => $this->handleMsg($op),
            Command::Ping => $this->handlePing(),
            Command::Pong => $this->handlePong(),
            Command::Ok => null, // Ignore in non-verbose mode
            Command::Err => $this->handleError($op),
            Command::Info => $this->handleInfo($op),
            default => null,
        };
    }

    private function handleMsg(ServerOp $op): void
    {
        $this->inMsgs++;

        if ($op->sid === null) {
            return;
        }
        $sub = $this->subscriptions[$op->sid] ?? null;
        if ($sub === null) {
            return;
        }

        // Parse headers if HMSG
        $headers = null;
        $body = $op->payload ?? '';
        if ($op->command === Command::HMsg && $op->headerBytes !== null && $op->headerBytes > 0) {
            $headerRaw = substr($body, 0, $op->headerBytes);
            $headers = Headers::fromWireFormat($headerRaw);
            $body = substr($body, $op->headerBytes);
        }

        $msg = new Message(
            subject: $op->subject ?? '',
            data: $body,
            replyTo: $op->replyTo,
            headers: $headers,
            connection: $this,
        );

        $sub->deliver($msg);
    }

    private function handlePing(): void
    {
        $this->writeToServer($this->writer->pong());
        $this->flushWrite();
    }

    private function handlePong(): void
    {
        $this->pingsOut = 0;

        // Fire first pending pong callback
        if ($this->pongCallbacks !== []) {
            $key = array_key_first($this->pongCallbacks);
            $callback = $this->pongCallbacks[$key];
            unset($this->pongCallbacks[$key]);
            $callback();
        }
    }

    private function handleError(ServerOp $op): void
    {
        $errMsg = $op->payload ?? 'Unknown error';
        $this->log('warning', 'Server error: {error}', ['error' => $errMsg]);
        $this->lastError = new NatsException("Server error: {$errMsg}");

        $handler = $this->onErrorHandler;
        if ($handler !== null) {
            $handler($this, $this->lastError);
            return;
        }

        if (str_contains($errMsg, 'Permissions Violation')) {
            throw new PermissionException($errMsg);
        }
        if (str_contains($errMsg, 'Authorization')) {
            throw new AuthorizationException($errMsg);
        }

        throw new NatsException("Server error: {$errMsg}");
    }

    private function handleInfo(ServerOp $op): void
    {
        if ($op->payload === null) {
            return;
        }

        $newInfo = ServerInfo::fromJson($op->payload);
        $this->serverInfo = $newInfo;
        $this->parser->setMaxPayload($newInfo->maxPayload);
        $this->log('debug', 'Received updated server info');

        // Update server pool with newly discovered servers
        if (!$this->options->ignoreDiscoveredServers) {
            foreach ($newInfo->connectUrls as $url) {
                $normalized = "nats://{$url}";
                if (!in_array($normalized, $this->serverPool, true)) {
                    $this->serverPool[] = $normalized;
                    $handler = $this->options->onDiscoveredServers;
                    if ($handler !== null) {
                        $handler($this);
                    }
                }
            }
        }

        // Lame duck mode
        if ($newInfo->lameDuckMode) {
            $this->log('warning', 'Server entered lame duck mode');
            $handler = $this->options->onLameDuckMode;
            if ($handler !== null) {
                $handler($this);
            }
        }
    }

    private function checkPing(): void
    {
        $now = microtime(true);
        if ($now - $this->lastPingTime < $this->options->pingInterval) {
            return;
        }

        if ($this->pingsOut >= $this->options->maxPingsOutstanding) {
            $this->log('warning', 'Max pings outstanding reached, disconnecting');
            $this->handleDisconnect();
            return;
        }

        $this->writeToServer($this->writer->ping());
        $this->flushWrite();
        $this->pingsOut++;
        $this->lastPingTime = $now;
    }

    // --- Internal: reconnect ---

    private function handleDisconnect(): void
    {
        $handler = $this->onDisconnectHandler;
        if ($handler !== null) {
            $handler($this);
        }

        if ($this->options->noReconnect) {
            $this->close();
            return;
        }

        $this->status = ConnectionStatus::Reconnecting;
        $this->parser->reset();
        $this->transport->close();
        $this->doReconnect();
    }

    private function doReconnect(): void
    {
        $maxReconnects = $this->options->maxReconnects;
        $attempts = 0;

        while ($maxReconnects < 0 || $attempts < $maxReconnects) {
            for ($i = 0; $i < count($this->serverPool); $i++) {
                $idx = ($this->currentServerIndex + 1 + $i) % count($this->serverPool);
                $url = $this->serverPool[$idx];

                try {
                    $this->transport = new TcpTransport();
                    $this->connectToServer($url);
                    $this->currentServerIndex = $idx;
                    $this->status = ConnectionStatus::Connected;
                    $this->reconnects++;

                    // Re-subscribe
                    foreach ($this->subscriptions as $sid => $sub) {
                        $cmd = $this->writer->sub($sub->subject(), $sid, $sub->queue());
                        $this->transport->write($cmd);
                    }
                    if ($this->subscriptions !== []) {
                        $this->log('debug', 'Re-subscribed {count} subscriptions', [
                            'count' => count($this->subscriptions),
                        ]);
                    }

                    // Flush reconnect buffer
                    if ($this->reconnectBuffer !== '') {
                        $this->transport->write($this->reconnectBuffer);
                        $this->reconnectBuffer = '';
                    }

                    $this->flushWrite();

                    $this->log('info', 'Reconnected to {url}', ['url' => $url]);

                    $handler = $this->onReconnectHandler;
                    if ($handler !== null) {
                        $handler($this);
                    }

                    return;
                } catch (\Throwable $e) {
                    $this->log('warning', 'Reconnect to {url} failed: {error}', [
                        'url' => $url,
                        'error' => $e->getMessage(),
                    ]);
                    continue;
                }
            }

            $attempts++;

            // Use custom reconnect delay if configured (matches Go CustomReconnectDelay)
            $customDelay = $this->options->customReconnectDelay;
            if ($customDelay !== null) {
                $result = $customDelay($attempts);
                $actualWait = is_numeric($result) ? (float) $result : 1.0;
            } else {
                $wait = $this->options->reconnectWait;
                $jitter = $this->options->tlsEnabled
                    ? $this->options->reconnectJitterTls
                    : $this->options->reconnectJitter;
                $actualWait = $wait + (mt_rand() / getrandmax()) * $jitter;
            }
            usleep((int) ($actualWait * 1_000_000));
        }

        $this->log('error', 'Max reconnection attempts reached');
        $this->close();
        throw new NatsException('Max reconnection attempts reached');
    }

    /**
     * @param array<string, mixed> $context
     */
    private function log(string $level, string $message, array $context = []): void
    {
        if ($this->logger === null) {
            return;
        }

        try {
            $this->logger->log($level, $message, $context);
        } catch (\Throwable) {
            // Logging must never change connection behavior.
        }
    }

    private function ensureConnected(): void
    {
        if ($this->status === ConnectionStatus::Closed) {
            throw new ConnectionClosedException();
        }
        if ($this->status !== ConnectionStatus::Connected && $this->status !== ConnectionStatus::Reconnecting) {
            throw new NatsException('Not connected');
        }
    }

    /**
     * Validates a subject string per NATS protocol rules.
     * Subjects cannot be empty or contain spaces, tabs, CR, or LF.
     */
    public static function validateSubject(string $subject): void
    {
        if ($subject === '') {
            throw new BadSubjectException('Subject cannot be empty');
        }

        // Check for whitespace and control characters — matches Go client behavior
        if (preg_match('/[\s\x00-\x1f\x7f]/', $subject)) {
            throw new BadSubjectException("Invalid subject: '{$subject}'");
        }
    }

    private function resolveServerPool(): void
    {
        $expanded = [];
        foreach ($this->serverPool as $url) {
            foreach ($this->resolveUrl($url) as $resolved) {
                if (!in_array($resolved, $expanded, true)) {
                    $expanded[] = $resolved;
                }
            }
        }
        $this->serverPool = $expanded;
    }

    /**
     * Resolves a single server URL to one or more URLs with IP addresses.
     *
     * @return list<string>
     */
    private function resolveUrl(string $url): array
    {
        $parsed = parse_url($url);
        if ($parsed === false) {
            return [$url];
        }

        $host = $parsed['host'] ?? null;
        if ($host === null) {
            return [$url];
        }

        // Already an IP address — no resolution needed
        if (filter_var($host, FILTER_VALIDATE_IP) !== false) {
            return [$url];
        }

        $ips = gethostbynamel($host);
        if ($ips === false || $ips === []) {
            return [$url];
        }

        $scheme = $parsed['scheme'] ?? 'nats';
        $port = $parsed['port'] ?? 4222;
        $userInfo = '';
        if (isset($parsed['user'])) {
            $userInfo = $parsed['user'];
            if (isset($parsed['pass'])) {
                $userInfo .= ':' . $parsed['pass'];
            }
            $userInfo .= '@';
        }

        $urls = [];
        foreach ($ips as $ip) {
            $urls[] = "{$scheme}://{$userInfo}{$ip}:{$port}";
        }

        return $urls;
    }

    /**
     * Checks if the message size exceeds the server's max payload.
     * Matches Go client: proactively rejects payloads over server threshold.
     */
    private function checkMaxPayload(int $msgSize): void
    {
        $maxPayload = $this->serverInfo !== null ? $this->serverInfo->maxPayload : 0;
        if ($maxPayload > 0 && $msgSize > $maxPayload) {
            throw new MaxPayloadException(
                "Maximum payload exceeded: {$msgSize} bytes > {$maxPayload} bytes"
            );
        }
    }
}
