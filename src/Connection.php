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

    // Reconnect buffer
    private string $reconnectBuffer = '';

    private function __construct() {}

    /**
     * @param string|list<string> $url
     */
    public static function connect(
        string|array $url = 'nats://127.0.0.1:4222',
        ?ConnectionOptions $options = null,
    ): self {
        $conn = new self();
        $conn->options = $options ?? new ConnectionOptions();
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
        $optServers = $conn->options->getServers();
        if ($optServers !== ['nats://127.0.0.1:4222'] || is_string($url) && $url === 'nats://127.0.0.1:4222') {
            if ($optServers !== ['nats://127.0.0.1:4222']) {
                $conn->serverPool = $optServers;
            }
        }

        // Randomize server pool unless disabled
        if (!$conn->options->isDontRandomize() && count($conn->serverPool) > 1) {
            shuffle($conn->serverPool);
        }

        $conn->doConnect();

        return $conn;
    }

    // --- Publishing ---

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

    public function subscribe(string $subject, \Closure $handler): Subscription
    {
        return $this->doSubscribe($subject, null, $handler);
    }

    public function subscribeSync(string $subject): Subscription
    {
        return $this->doSubscribe($subject, null, null);
    }

    public function queueSubscribe(string $subject, string $queue, \Closure $handler): Subscription
    {
        return $this->doSubscribe($subject, $queue, $handler);
    }

    public function queueSubscribeSync(string $subject, string $queue): Subscription
    {
        return $this->doSubscribe($subject, $queue, null);
    }

    // --- Request-Reply ---

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
        $this->flushWrite();
        $this->transport->close();
        $this->parser->reset();
        $this->subscriptions = [];

        $handler = $this->options->getOnClose();
        if ($handler !== null) {
            $handler($this);
        }
    }

    public function drain(): void
    {
        $this->status = ConnectionStatus::DrainingSubscriptions;

        // Unsubscribe all
        foreach ($this->subscriptions as $sub) {
            $sub->drain();
        }

        $this->status = ConnectionStatus::DrainingPublications;
        $this->flush();
        $this->close();
    }

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
        $this->options->setOnDisconnect($handler);
    }

    /**
     * Dynamically set the reconnect handler on the connection.
     */
    public function setReconnectHandler(?\Closure $handler): void
    {
        $this->options->setOnReconnect($handler);
    }

    /**
     * Dynamically set the closed handler on the connection.
     */
    public function setClosedHandler(?\Closure $handler): void
    {
        $this->options->setOnClose($handler);
    }

    /**
     * Dynamically set the error handler on the connection.
     */
    public function setErrorHandler(?\Closure $handler): void
    {
        $this->options->setOnError($handler);
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
        return Inbox::generate($this->options->getInboxPrefix());
    }

    // --- JetStream ---

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

                $handler = $this->options->getOnConnect();
                if ($handler !== null) {
                    $handler($this);
                }

                return;
            } catch (\Throwable $e) {
                $lastError = $e;
                continue;
            }
        }

        $this->status = ConnectionStatus::Disconnected;
        throw new NatsException(
            'Failed to connect to any NATS server: ' . ($lastError?->getMessage() ?? 'unknown error')
        );
    }

    private function connectToServer(string $url): void
    {
        $parsed = parse_url($url);
        $host = $parsed['host'] ?? '127.0.0.1';
        $port = $parsed['port'] ?? 4222;
        $scheme = $parsed['scheme'] ?? 'nats';

        $useTls = $scheme === 'tls' || $scheme === 'nats+tls' || $this->options->isTlsEnabled();
        $address = "tcp://{$host}:{$port}";

        $tlsContext = $useTls ? $this->options->getTlsContext() : null;

        $this->transport->connect($address, $this->options->getTimeout(), $tlsContext);
        $this->transport->setTimeout($this->options->getTimeout());

        // Read INFO
        $infoLine = $this->transport->readLine();
        $infoLine = rtrim($infoLine, "\r\n");

        if (!str_starts_with($infoLine, 'INFO ')) {
            throw new NatsException("Expected INFO, got: {$infoLine}");
        }

        $this->serverInfo = ServerInfo::fromJson(substr($infoLine, 5));
        $this->parser->setMaxPayload($this->serverInfo->maxPayload);

        // TLS upgrade if server requires it but we connected plain
        if ($this->serverInfo->tlsRequired && !$useTls) {
            $this->transport->upgradeTls($this->options->getTlsContext());
        }

        // Add discovered servers to pool
        if (!$this->options->isIgnoreDiscoveredServers()) {
            foreach ($this->serverInfo->connectUrls as $discoveredUrl) {
                $normalized = "nats://{$discoveredUrl}";
                if (!in_array($normalized, $this->serverPool, true)) {
                    $this->serverPool[] = $normalized;
                }
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
        if (isset($parsed['user']) && $this->options->getAuthenticator() === null) {
            // URL had embedded credentials — already handled in buildConnectPayload
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function buildConnectPayload(): array
    {
        $payload = [
            'verbose' => $this->options->isVerbose(),
            'pedantic' => $this->options->isPedantic(),
            'lang' => 'php',
            'version' => '1.0.0',
            'protocol' => 1,
            'headers' => true,
            'no_responders' => true,
        ];

        if ($this->options->getName() !== null) {
            $payload['name'] = $this->options->getName();
        }

        if ($this->options->isNoEcho()) {
            $payload['echo'] = false;
        }

        // Auth
        $auth = $this->options->getAuthenticator();
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
        if (isset($parsed['user']) && $auth === null) {
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

        return $sub;
    }

    /** @internal */
    public function unsubscribeSid(string $sid, ?int $maxMessages = null): void
    {
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
        $this->lastError = new SlowConsumerException(
            "Slow consumer on subject '{$sub->subject()}': messages dropped"
        );

        $handler = $this->options->getOnError();
        if ($handler !== null) {
            $handler($this, $this->lastError);
        }
    }

    // --- Internal: I/O ---

    private function writeToServer(string $data): void
    {
        if ($this->status === ConnectionStatus::Reconnecting) {
            $maxSize = $this->options->getReconnectBufferSize();
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
        $this->lastError = new NatsException("Server error: {$errMsg}");

        $handler = $this->options->getOnError();
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

        // Update server pool with newly discovered servers
        if (!$this->options->isIgnoreDiscoveredServers()) {
            foreach ($newInfo->connectUrls as $url) {
                $normalized = "nats://{$url}";
                if (!in_array($normalized, $this->serverPool, true)) {
                    $this->serverPool[] = $normalized;
                    $handler = $this->options->getOnDiscoveredServers();
                    if ($handler !== null) {
                        $handler($this);
                    }
                }
            }
        }

        // Lame duck mode
        if ($newInfo->lameDuckMode) {
            $handler = $this->options->getOnLameDuckMode();
            if ($handler !== null) {
                $handler($this);
            }
        }
    }

    private function checkPing(): void
    {
        $now = microtime(true);
        if ($now - $this->lastPingTime < $this->options->getPingInterval()) {
            return;
        }

        if ($this->pingsOut >= $this->options->getMaxPingsOutstanding()) {
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
        $handler = $this->options->getOnDisconnect();
        if ($handler !== null) {
            $handler($this);
        }

        if ($this->options->isNoReconnect()) {
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
        $maxReconnects = $this->options->getMaxReconnects();
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

                    // Flush reconnect buffer
                    if ($this->reconnectBuffer !== '') {
                        $this->transport->write($this->reconnectBuffer);
                        $this->reconnectBuffer = '';
                    }

                    $this->flushWrite();

                    $handler = $this->options->getOnReconnect();
                    if ($handler !== null) {
                        $handler($this);
                    }

                    return;
                } catch (\Throwable) {
                    continue;
                }
            }

            $attempts++;

            // Use custom reconnect delay if configured (matches Go CustomReconnectDelay)
            $customDelay = $this->options->getCustomReconnectDelay();
            if ($customDelay !== null) {
                $result = $customDelay($attempts);
                $actualWait = is_numeric($result) ? (float) $result : 1.0;
            } else {
                $wait = $this->options->getReconnectWait();
                $jitter = $this->options->isTlsEnabled()
                    ? $this->options->getReconnectJitterTls()
                    : $this->options->getReconnectJitter();
                $actualWait = $wait + (mt_rand() / getrandmax()) * $jitter;
            }
            usleep((int) ($actualWait * 1_000_000));
        }

        $this->close();
        throw new NatsException('Max reconnection attempts reached');
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
