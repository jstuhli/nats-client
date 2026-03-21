# nats-client

Modern PHP 8.5 client library for [NATS](https://nats.io) messaging system. Zero runtime dependencies.

Covers the full NATS feature set: Core pub/sub, request/reply, JetStream (streams, consumers, publish), Key-Value store, Object Store, and Micro service framework.

## Requirements

- PHP >= 8.5
- `ext-sodium` (bundled with PHP)

## Installation

```bash
composer require matkomat/nats-client
```

## Quick Start

```php
use Nats\Connection;
use Nats\Message;

// Connect
$conn = Connection::connect(); // localhost:4222

// Publish
$conn->publish('greetings', 'Hello NATS!');

// Subscribe
$conn->subscribe('greetings', function (Message $msg) {
    echo "{$msg->subject}: {$msg->data}\n";
});

// Request/Reply
$reply = $conn->request('math.add', '{"a":5,"b":3}', timeout: 2.0);

// JetStream
$js = $conn->jetStream();
$kv = $js->createKeyValue(new \Nats\KeyValue\KeyValueConfig(bucket: 'cache'));
$kv->put('key', 'value');
$entry = $kv->get('key');

$conn->close();
```

## Features

| Feature | Description |
|---|---|
| **Core NATS** | Publish, subscribe (sync/async), request/reply, queue groups, wildcards, RTT, barrier |
| **Headers** | HTTP-style message headers (NATS/1.0 wire format) |
| **Authentication** | User/pass, token, NKey (Ed25519/sodium), JWT, credentials file, TLS |
| **Reconnection** | Automatic reconnect with jitter, buffer, custom delay, server discovery, dynamic handlers |
| **Event Handlers** | onConnect, onDisconnect, onReconnect, onClose, onError, onLameDuckMode, onDiscoveredServers |
| **JetStream Streams** | Create, createOrUpdate, update, delete, purge, info (with filters), list, getMessage, secureDelete, mirrors, sources, subject transforms |
| **JetStream Consumers** | Pull (fetch/fetchBytes/fetchNoWait/next/messages/consume), push, ordered, durable, pause/resume, priority groups |
| **JetStream Publish** | Sync/async (with pending tracking), deduplication (msgId), per-message TTL, optimistic concurrency, retry, stallWait |
| **Key-Value Store** | Put, get, getRevision, create (CAS + TTL), update (CAS), delete, purge (+ TTL), purgeDeletes, history, watch, watchFiltered, listKeys, listKeysFiltered, bucket CRUD |
| **Object Store** | Put (bytes/string/file), get, getInfo, delete, list (+ showDeleted), addLink, addBucketLink, seal, watch, chunked transfer, metadata, compression |
| **Micro Services** | Endpoints, groups, discovery ($SRV.PING/INFO/STATS), statistics, pending limits, handler classes |

## Architecture

- **`readonly` classes** for all immutable data (configs, entries, info, metadata)
- **Backed string enums** for all policy types — JSON serialize/deserialize friendly
- **Fiber-based** async operations (publishAsync, consume, messages, watch)
- **Generator/IteratorAggregate** for streaming and pagination
- **`stream_socket_client`** for TCP — zero dependencies, non-blocking I/O, TLS
- **`sodium_crypto_sign_detached`** for NKey auth — bundled in PHP

## Examples

```
docs/examples/
├── core/
│   ├── 01-connect.php           # Connection variants (default, URL, cluster, options)
│   ├── 02-auth.php              # User/pass, token, NKey, JWT, TLS
│   ├── 03-pub-sub.php           # Publish, subscribe (async/sync), wildcards, queue groups
│   ├── 04-request-reply.php     # Synchronous RPC, request with headers
│   ├── 05-headers.php           # Set/get/iterate headers, wire format
│   ├── 06-events-reconnect.php  # Event handlers, reconnect config, custom backoff, forceReconnect
│   ├── 07-connection-info.php   # RTT, addresses, client info, serverInfo, server pool, barrier
│   ├── 08-subscriptions-advanced.php  # Pending limits, max pending, queued msgs, closed handler
│   ├── 09-error-handling.php    # Exception hierarchy, no-responders, JetStream errors, retry
│   ├── 10-safeguards.php        # Max payload, subject validation, slow consumer detection
│   └── 11-graceful-shutdown.php # Drain vs close, lame duck, signal handling, status transitions
│
├── jetstream/
│   ├── 01-streams.php           # Create, info, getMessage, purge, list, delete
│   ├── 02-consumers.php         # Pull fetch/next/messages/consume, ConsumeContext, ordered, ACK variants
│   ├── 03-publish.php           # Deduplication, optimistic concurrency, async publish
│   ├── 04-streams-advanced.php  # CreateOrUpdate, streamNameBySubject, secureDelete, mirrors, sources, subject transforms
│   ├── 05-consumers-advanced.php  # Pause/resume, push consumers, backoff, fetchBytes, headersOnly
│   ├── 06-publish-advanced.php  # Async lifecycle, msgTtl, expectLastMsgId, stallWait, publishMessage
│   └── 07-direct-get.php       # Direct message access, account info, fast lookups
│
├── keyvalue/
│   ├── 01-basic.php             # Put/get/create/update/delete/purge, history, listKeys, status
│   ├── 02-watch.php             # Real-time watching, patterns, updatesOnly, includeHistory
│   └── 03-advanced.php          # getRevision, keyTtl, listKeysFiltered, watchFiltered, bucket management
│
├── objectstore/
│   ├── 01-basic.php             # Put bytes/string/file, get, delete, list, metadata, status
│   └── 02-advanced.php          # getInfo, addLink, addBucketLink, seal, watch, showDeleted, compression
│
└── micro/
    └── 01-service.php           # Endpoints, groups, discovery, handler classes, statistics
```

### Core — Connect

```php
use Nats\Connection;
use Nats\ConnectionOptions;

// Default (localhost:4222)
$conn = Connection::connect();

// Multiple servers (automatic failover)
$conn = Connection::connect(['nats://srv1:4222', 'nats://srv2:4222']);

// With options
$options = (new ConnectionOptions())
    ->name('my-app')
    ->timeout(5.0)
    ->maxReconnects(10)
    ->reconnectWait(1.0);

$conn = Connection::connect('nats://localhost:4222', $options);
```

### Core — Pub/Sub

```php
// Async subscribe (callback)
$conn->subscribe('orders.>', function (Message $msg) {
    echo "{$msg->subject}: {$msg->data}\n";
});

// Sync subscribe
$sub = $conn->subscribeSync('events.*');
$msg = $sub->nextMessage(timeout: 1.0);

// Queue groups (load balancing)
$conn->queueSubscribe('tasks', 'workers', function (Message $msg) {
    echo "Processing: {$msg->data}\n";
});

// Auto-unsubscribe after N messages
$sub->autoUnsubscribe(100);
```

### Core — Request/Reply

```php
$conn->subscribe('math.add', function (Message $msg) {
    $data = json_decode($msg->data, true);
    $msg->respond(json_encode(['result' => $data['a'] + $data['b']]));
});

$reply = $conn->request('math.add', '{"a":5,"b":3}', timeout: 2.0);
echo json_decode($reply->data, true)['result']; // 8
```

### Core — Headers

```php
use Nats\Headers;
use Nats\Message;

$headers = new Headers(['Content-Type' => 'application/json', 'X-Trace' => 'abc']);
$msg = new Message(subject: 'events.order', data: '{}', headers: $headers);
$conn->publishMessage($msg);
```

### Core — Authentication

```php
// User/Password
$options = (new ConnectionOptions())->userInfo('user', 'pass');

// Token
$options = (new ConnectionOptions())->token('s3cr3t');

// NKey (Ed25519)
$options = (new ConnectionOptions())->nkey($nkeySeed);

// Credentials file
$options = (new ConnectionOptions())->credentials('/path/to/user.creds');

// TLS
$options = (new ConnectionOptions())
    ->tls()
    ->tlsCertificate('/path/to/cert.pem', '/path/to/key.pem')
    ->tlsCaCertificate('/path/to/ca.pem');
```

### Core — Events & Reconnect

```php
$options = (new ConnectionOptions())
    ->maxReconnects(-1)              // Infinite
    ->reconnectWait(2.0)
    ->reconnectJitter(0.5, 1.0)
    ->reconnectBufferSize(8 * 1024 * 1024)
    ->onConnect(fn(Connection $c) => echo "Connected to {$c->connectedUrl()}\n")
    ->onDisconnect(fn(Connection $c) => echo "Disconnected!\n")
    ->onReconnect(fn(Connection $c) => echo "Reconnected!\n")
    ->onError(fn(Connection $c, \Throwable $e) => echo "Error: {$e->getMessage()}\n");
```

### JetStream — Streams

```php
use Nats\JetStream\Stream\StreamConfig;
use Nats\Enum\{RetentionPolicy, StorageType, DiscardPolicy};

$js = $conn->jetStream();

$stream = $js->createStream(new StreamConfig(
    name: 'ORDERS',
    subjects: ['orders.>'],
    retention: RetentionPolicy::Limits,
    maxAge: 86400,
    maxBytes: 100 * 1024 * 1024,
    storage: StorageType::File,
    discard: DiscardPolicy::Old,
));

$ack = $js->publish('orders.new', '{"id": 1}');
echo "seq={$ack->sequence}\n";

$rawMsg = $stream->getMessage(1);
$stream->purge();
```

### JetStream — Consumers

```php
use Nats\JetStream\Consumer\{ConsumerConfig, FetchOptions, ConsumeOptions};
use Nats\Enum\{DeliveryPolicy, AckPolicy};

$consumer = $stream->createOrUpdateConsumer(new ConsumerConfig(
    name: 'processor',
    durable: 'processor',
    deliverPolicy: DeliveryPolicy::All,
    ackPolicy: AckPolicy::Explicit,
    filterSubject: 'orders.>',
));

// Fetch batch
$batch = $consumer->fetch(10, new FetchOptions(timeout: 2.0));
foreach ($batch as $msg) {
    echo $msg->data() . "\n";
    $msg->ack();
}

// Single message
$msg = $consumer->next(timeout: 2.0);
$msg->ack();

// Iterator (infinite)
$msgs = $consumer->messages(new ConsumeOptions(maxMessages: 100, expires: 5.0));
foreach ($msgs as $msg) {
    $msg->ack();
}

// ACK variants: ack(), ackSync(), nak(), nakWithDelay(5.0), term(), inProgress()
```

### JetStream — Publish Options

```php
use Nats\JetStream\Publish\PublishOptions;

// Deduplication
$js->publish('orders.new', $data, PublishOptions::msgId('order-1'));

// Optimistic concurrency
$js->publish('orders.new', $data, PublishOptions::expectLastSequence(42));
$js->publish('orders.new', $data, PublishOptions::expectStream('ORDERS'));

// Async
$future = $js->publishAsync('orders.new', $data);
$conn->process(0.5);
$ack = $future->ok();
```

### Key-Value Store

```php
use Nats\KeyValue\KeyValueConfig;

$kv = $js->createKeyValue(new KeyValueConfig(
    bucket: 'settings',
    history: 5,
    ttl: 3600.0,
));

// CRUD
$revision = $kv->put('theme', 'dark');
$entry = $kv->get('theme');          // KeyValueEntry{key, value, revision, operation, timestamp}
$rev = $kv->create('lang', 'en');    // Fails if exists
$rev = $kv->update('theme', 'light', $entry->revision);  // CAS
$kv->delete('lang');
$kv->purge('theme');

// History & keys
$history = $kv->history('counter');  // KeyValueEntry[]
$keys = $kv->listKeys();            // string[]

// Watch
$watcher = $kv->watch('>', WatchOptions::updatesOnly());
foreach ($watcher as $entry) {
    echo "{$entry->key} = {$entry->value}\n";
}
```

### Object Store

```php
use Nats\ObjectStore\ObjectStoreConfig;

$store = $js->createObjectStore(new ObjectStoreConfig(
    bucket: 'files',
    maxBytes: 100 * 1024 * 1024,
    maxChunkSize: 131072,
));

// Store
$info = $store->putBytes('config.json', $jsonBytes);
$info = $store->putString('readme.txt', 'Hello!');
// $info = $store->putFile('/path/to/file.zip');

// Retrieve
$content = $store->getBytes('config.json');
$text = $store->getString('readme.txt');

// List & delete
$objects = $store->list();           // ObjectInfo[]
$store->delete('config.json');
```

### Micro Services

```php
use Nats\Micro\{Service, ServiceConfig, EndpointConfig, RequestInterface};

$svc = Service::create($conn, new ServiceConfig(
    name: 'calculator',
    version: '1.0.0',
));

// Add endpoints
$svc->addEndpoint('add', function (RequestInterface $req) {
    $data = json_decode($req->data(), true);
    $req->respondJson(['result' => $data['a'] + $data['b']]);
}, new EndpointConfig(subject: 'calc.add'));

// Groups
$math = $svc->addGroup('math');
$math->addEndpoint('sqrt', function (RequestInterface $req) {
    $req->respondJson(['result' => sqrt(json_decode($req->data(), true)['n'])]);
});

// Auto-discovery: $SRV.PING, $SRV.INFO.calculator, $SRV.STATS.calculator
$info = $svc->info();
$stats = $svc->stats();
$svc->stop();
```

## Enums

All policy types are backed string enums for type safety and JSON compatibility:

| Enum | Values |
|---|---|
| `ConnectionStatus` | `Connected`, `Disconnected`, `Closed`, `Reconnecting`, `Draining` |
| `RetentionPolicy` | `Limits`, `Interest`, `WorkQueue` |
| `DeliveryPolicy` | `All`, `Last`, `New`, `ByStartSequence`, `ByStartTime`, `LastPerSubject` |
| `AckPolicy` | `Explicit`, `All`, `None` |
| `StorageType` | `File`, `Memory` |
| `DiscardPolicy` | `Old`, `New` |
| `ReplayPolicy` | `Instant`, `Original` |
| `StoreCompression` | `None`, `S2` |
| `KeyValueOperation` | `Put`, `Delete`, `Purge` |
| `ServiceVerb` | `Ping`, `Stats`, `Info` |

## Testing

```bash
# Unit tests
vendor/bin/phpunit --testsuite=unit

# Integration tests (requires NATS server)
docker compose up -d
vendor/bin/phpunit --testsuite=integration

# Static analysis
vendor/bin/phpstan analyse src/
```

## Docker

The project includes a `docker-compose.yml` for integration testing:

```bash
docker compose up -d    # Starts nats-server on port 4222
docker compose down     # Stop
```

## Project Structure

```
src/
├── Connection.php              # Main class — connect, pub/sub, request, event loop
├── ConnectionOptions.php       # Fluent builder for connection options
├── Subscription.php            # Sync/async subscriptions
├── Message.php                 # readonly — Subject, Data, ReplyTo, Headers
├── Headers.php                 # Header map (IteratorAggregate, Countable)
├── Inbox.php                   # _INBOX.<nuid> generator
├── ServerInfo.php              # readonly — INFO payload
├── Statistics.php              # readonly — in/out msgs/bytes/reconnects
├── NatsException.php           # Base exception
│
├── Enum/                       # Backed string enums
├── Protocol/                   # Parser (state machine), Writer (formatters)
├── Transport/                  # TCP/TLS transports, ring buffer
├── Auth/                       # UserPass, Token, NKey, JWT, Credentials
├── JetStream/                  # Streams, consumers, publish, iterators
│   ├── Stream/                 # StreamConfig, StreamInfo, Stream
│   ├── Consumer/               # ConsumerConfig, fetch/consume/messages
│   ├── Publish/                # PubAck, PubAckFuture, PublishOptions
│   ├── Message/                # JetStreamMessage (ack/nak/term)
│   └── Iterator/               # Paginated list iterators
├── KeyValue/                   # KV store, watch, history
├── ObjectStore/                # Chunked object storage
└── Micro/                      # Service framework, discovery
```

## License

MIT
