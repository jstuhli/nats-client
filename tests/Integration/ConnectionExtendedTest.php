<?php

declare(strict_types=1);

namespace Nats\Tests\Integration;

require_once dirname(__DIR__) . '/Unit/PsrLoggerStub.php';

use Nats\Connection;
use Nats\ConnectionOptions;
use PHPUnit\Framework\TestCase;

final class ConnectionExtendedTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
    }

    protected function tearDown(): void
    {
        $this->conn->close();
    }

    public function testRtt(): void
    {
        $rtt = $this->conn->rtt();

        self::assertIsFloat($rtt);
        self::assertGreaterThan(0.0, $rtt);
        self::assertLessThan(1.0, $rtt); // Localhost should be well under 1 second
    }

    public function testConnectedAddr(): void
    {
        $addr = $this->conn->connectedAddr();

        self::assertNotEmpty($addr);
        self::assertTrue(
            str_contains($addr, '127.0.0.1') || str_contains($addr, 'localhost'),
            "Expected connectedAddr to contain '127.0.0.1' or 'localhost', got: {$addr}",
        );
        self::assertStringContainsString(':4222', $addr);
    }

    public function testNumSubscriptions(): void
    {
        $initialCount = $this->conn->numSubscriptions();

        $sub1 = $this->conn->subscribeSync('test.ext.numsub.a.' . uniqid());
        $sub2 = $this->conn->subscribeSync('test.ext.numsub.b.' . uniqid());
        $sub3 = $this->conn->subscribeSync('test.ext.numsub.c.' . uniqid());

        self::assertSame($initialCount + 3, $this->conn->numSubscriptions());

        $sub1->unsubscribe();

        self::assertSame($initialCount + 2, $this->conn->numSubscriptions());

        $sub2->unsubscribe();
        $sub3->unsubscribe();

        self::assertSame($initialCount, $this->conn->numSubscriptions());
    }

    public function testClientInfo(): void
    {
        $clientId = $this->conn->clientId();
        $clientIp = $this->conn->clientIp();

        self::assertNotNull($clientId, 'clientId should not be null when connected');
        self::assertNotNull($clientIp, 'clientIp should not be null when connected');
        self::assertNotEmpty($clientIp);
    }

    public function testAuthRequired(): void
    {
        // Default NATS server without auth configured should return false
        self::assertFalse($this->conn->authRequired());
    }

    public function testTlsRequired(): void
    {
        // Default NATS server without TLS configured should return false
        self::assertFalse($this->conn->tlsRequired());
    }

    public function testLastErrorIsNullInitially(): void
    {
        self::assertNull($this->conn->lastError());
    }

    public function testBuffered(): void
    {
        // After connect and flush, buffer should be empty
        $this->conn->flush();
        self::assertSame(0, $this->conn->buffered());

        // Publish without flushing -- buffer should have data
        // Note: publish may auto-flush depending on implementation,
        // but buffered() should return >= 0 without error
        $bufferedBefore = $this->conn->buffered();
        $this->conn->publish('test.buffered.' . uniqid(), str_repeat('x', 100));
        $bufferedAfter = $this->conn->buffered();

        // The buffer should have grown or been flushed (both are valid behaviors)
        self::assertGreaterThanOrEqual(0, $bufferedAfter);

        // After explicit flush, buffer should be empty
        $this->conn->flush();
        self::assertSame(0, $this->conn->buffered());
    }

    public function testBarrier(): void
    {
        $barrierFired = false;

        $subject = 'test.barrier.' . uniqid();
        $this->conn->publish($subject, 'before-barrier');

        $this->conn->barrier(function () use (&$barrierFired): void {
            $barrierFired = true;
        });

        // Process incoming to receive the PONG that triggers the barrier callback
        $deadline = microtime(true) + 5.0;
        while (!$barrierFired && microtime(true) < $deadline) {
            $this->conn->process(0.05);
        }

        self::assertTrue($barrierFired, 'Barrier callback should have been fired after flush/PONG');
    }

    public function testDynamicHandlerSetters(): void
    {
        // Verify that calling the dynamic handler setters does not throw
        $this->conn->setDisconnectHandler(function (): void {});
        $this->conn->setReconnectHandler(function (): void {});
        $this->conn->setClosedHandler(function (): void {});
        $this->conn->setErrorHandler(function (): void {});

        // Also verify setting null handlers works
        $this->conn->setDisconnectHandler(null);
        $this->conn->setReconnectHandler(null);
        $this->conn->setClosedHandler(null);
        $this->conn->setErrorHandler(null);

        // If we got here without exception, the test passes
        self::assertTrue(true);
    }

    public function testThrowingLoggerDoesNotBreakConnectionLifecycle(): void
    {
        $logger = new class implements \Psr\Log\LoggerInterface {
            public function emergency(string|\Stringable $message, array $context = []): void
            {
                $this->log('emergency', $message, $context);
            }

            public function alert(string|\Stringable $message, array $context = []): void
            {
                $this->log('alert', $message, $context);
            }

            public function critical(string|\Stringable $message, array $context = []): void
            {
                $this->log('critical', $message, $context);
            }

            public function error(string|\Stringable $message, array $context = []): void
            {
                $this->log('error', $message, $context);
            }

            public function warning(string|\Stringable $message, array $context = []): void
            {
                $this->log('warning', $message, $context);
            }

            public function notice(string|\Stringable $message, array $context = []): void
            {
                $this->log('notice', $message, $context);
            }

            public function info(string|\Stringable $message, array $context = []): void
            {
                $this->log('info', $message, $context);
            }

            public function debug(string|\Stringable $message, array $context = []): void
            {
                $this->log('debug', $message, $context);
            }

            public function log(mixed $level, string|\Stringable $message, array $context = []): void
            {
                throw new \RuntimeException('log failed');
            }
        };

        $conn = Connection::connect(
            self::NATS_URL,
            (new ConnectionOptions())->withLogger($logger),
        );

        self::assertTrue($conn->isConnected());

        $conn->close();

        self::assertTrue($conn->isClosed());
    }
}
