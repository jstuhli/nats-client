<?php

/**
 * Error handling patterns.
 *
 * Demonstrates the exception hierarchy, JetStream API errors,
 * the no-responders pattern, the error callback, and retry logic.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\BadSubjectException;
use Nats\Connection;
use Nats\ConnectionClosedException;
use Nats\ConnectionOptions;
use Nats\MaxPayloadException;
use Nats\NatsException;
use Nats\SlowConsumerException;
use Nats\TimeoutException;
use Nats\JetStream\Error\JetStreamException;
use Nats\JetStream\Stream\StreamConfig;

// --- 1. Global error callback ---
// Asynchronous errors (slow consumer, permission violations, etc.)
// are delivered here instead of being thrown inline.

$options = (new ConnectionOptions())
    ->name('error-handling-demo')
    ->onError(function (Connection $conn, \Throwable $err) {
        // Log the error, increment a metric, alert ops — whatever makes sense
        echo "[onError] {$err::class}: {$err->getMessage()}\n";

        // The same error is also available afterwards:
        // $conn->lastError()
    });

$conn = Connection::connect('nats://localhost:4222', $options);

// --- 2. MaxPayloadException ---
// The client checks payload size *before* sending. No bytes hit the wire.

$maxBytes = $conn->maxPayload();
echo "Server max payload: {$maxBytes} bytes\n";

try {
    $conn->publish('demo.big', str_repeat('x', $maxBytes + 1));
} catch (MaxPayloadException $e) {
    echo "Caught MaxPayloadException: {$e->getMessage()}\n";
}

// --- 3. BadSubjectException ---
// Subjects containing spaces, tabs, newlines, or empty strings are rejected.

try {
    $conn->publish('bad subject', 'data');
} catch (BadSubjectException $e) {
    echo "Caught BadSubjectException: {$e->getMessage()}\n";
}

// --- 4. TimeoutException ---
// Thrown when a sync operation (nextMessage, request, flush) exceeds its deadline.

$sub = $conn->subscribeSync('demo.timeout');
try {
    $sub->nextMessage(timeout: 0.1);
} catch (TimeoutException $e) {
    echo "Caught TimeoutException: {$e->getMessage()}\n";
}
$sub->unsubscribe();

// --- 5. No-responders pattern ---
// When nobody is listening on the request subject the server returns
// a 503 status header immediately (requires no_responders server feature).
// The client raises a NatsException instead of waiting until timeout.

try {
    $conn->request('demo.nobody.' . uniqid(), 'hello', 0.5);
} catch (NatsException $e) {
    echo "No responders: {$e->getMessage()}\n";
}

// --- 6. ConnectionClosedException ---
// Publishing (or any I/O) after close() throws immediately.

$conn2 = Connection::connect();
$conn2->close();
try {
    $conn2->publish('demo.closed', 'data');
} catch (ConnectionClosedException $e) {
    echo "Caught ConnectionClosedException: {$e->getMessage()}\n";
}

// --- 7. JetStreamException + ApiError ---
// JetStream operations that fail on the server return structured errors
// with a numeric error code, an HTTP-style status code, and a description.

$js = $conn->jetStream();

// Create a stream so we can provoke a known error:
$js->createStream(new StreamConfig(name: 'ERR_DEMO', subjects: ['err.>']));

try {
    // Trying to create a stream with a name that already exists
    // but different subjects triggers a conflict error.
    $js->createStream(new StreamConfig(name: 'ERR_DEMO', subjects: ['different.>']));
} catch (JetStreamException $e) {
    echo "JetStreamException: {$e->getMessage()}\n";

    if ($e->apiError !== null) {
        echo "  HTTP code : {$e->apiError->code}\n";        // e.g. 400
        echo "  NATS code : {$e->apiError->errCode}\n";     // e.g. 10058
        echo "  Description: {$e->apiError->description}\n";
    }
}

$js->deleteStream('ERR_DEMO');

// --- 8. Retry pattern with backoff ---
// Transient errors (TimeoutException, "no responders") are worth retrying.
// Permanent errors (BadSubjectException, MaxPayloadException) are not.

function publishWithRetry(
    Connection $conn,
    string $subject,
    string $data,
    int $maxAttempts = 3,
    float $baseDelay = 0.1,
): void {
    for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
        try {
            $conn->publish($subject, $data);
            return; // success
        } catch (TimeoutException | SlowConsumerException $e) {
            // Transient — wait with exponential backoff, then retry
            if ($attempt === $maxAttempts) {
                throw $e;
            }
            $delay = $baseDelay * (2 ** ($attempt - 1));
            echo "  Attempt {$attempt} failed ({$e::class}), retrying in {$delay}s ...\n";
            usleep((int) ($delay * 1_000_000));
        }
        // BadSubjectException, MaxPayloadException, ConnectionClosedException
        // are NOT caught — they bubble up immediately because retrying won't help.
    }
}

echo "\nPublish with retry:\n";
publishWithRetry($conn, 'demo.retry', 'hello');
echo "  Published successfully.\n";

// --- 9. Last error ---
// Between callbacks, you can inspect the most recent async error:

$lastError = $conn->lastError();
echo "\nLast error: " . ($lastError !== null ? $lastError->getMessage() : 'none') . "\n";

$conn->close();
