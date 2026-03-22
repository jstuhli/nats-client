<?php

declare(strict_types=1);

namespace Nats\Tests\Integration;

use Nats\Connection;
use Nats\ConnectionOptions;
use Nats\Enum\ConnectionStatus;
use PHPUnit\Framework\TestCase;

final class ConnectionTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';

    public function testConnectDefault(): void
    {
        $conn = Connection::connect(self::NATS_URL);

        self::assertTrue($conn->isConnected());
        self::assertSame(ConnectionStatus::Connected, $conn->status());
        self::assertNotEmpty($conn->connectedUrl());
        self::assertNotEmpty($conn->connectedServerVersion());
        self::assertGreaterThan(0, $conn->maxPayload());
        self::assertTrue($conn->headersSupported());

        $conn->close();
        self::assertTrue($conn->isClosed());
    }

    public function testConnectWithName(): void
    {
        $options = (new ConnectionOptions())->withName('test-app');
        $conn = Connection::connect(self::NATS_URL, $options);

        self::assertTrue($conn->isConnected());
        $conn->close();
    }

    public function testConnectFailsWithBadUrl(): void
    {
        $this->expectException(\Nats\NatsException::class);
        Connection::connect('nats://127.0.0.1:19999');
    }

    public function testConnectFailsFastWithMalformedUrl(): void
    {
        $badUrl = 'nats://user@:4222';

        $this->expectException(\Nats\NatsException::class);
        $this->expectExceptionMessage("Failed to connect to any NATS server: Invalid server URL: {$badUrl}");

        Connection::connect($badUrl);
    }

    public function testConnectSkipsMalformedUrlWhenValidServerIsAvailable(): void
    {
        $badUrl = 'nats://user@:4222';
        $options = (new ConnectionOptions())->withDontRandomize();

        $conn = Connection::connect([$badUrl, self::NATS_URL], $options);

        self::assertTrue($conn->isConnected());
        self::assertSame(self::NATS_URL, $conn->connectedUrl());

        $conn->close();
    }

    public function testFlush(): void
    {
        $conn = Connection::connect(self::NATS_URL);
        $conn->flush(2.0);
        self::assertTrue($conn->isConnected());
        $conn->close();
    }

    public function testServerInfo(): void
    {
        $conn = Connection::connect(self::NATS_URL);
        $info = $conn->serverInfo();

        self::assertNotNull($info);
        self::assertNotEmpty($info->serverId);
        self::assertNotEmpty($info->version);
        self::assertTrue($info->jetStream);
        self::assertGreaterThan(0, $info->maxPayload);
        self::assertGreaterThanOrEqual(1, $info->proto);

        $conn->close();
    }

    public function testStats(): void
    {
        $conn = Connection::connect(self::NATS_URL);

        $conn->publish('test.stats', 'hello');
        $conn->flush();

        $stats = $conn->stats();
        self::assertGreaterThanOrEqual(1, $stats->outMsgs);
        self::assertGreaterThan(0, $stats->outBytes);
        self::assertSame(0, $stats->reconnects);

        $conn->close();
    }

    public function testNewInbox(): void
    {
        $conn = Connection::connect(self::NATS_URL);

        $inbox1 = $conn->newInbox();
        $inbox2 = $conn->newInbox();

        self::assertStringStartsWith('_INBOX.', $inbox1);
        self::assertStringStartsWith('_INBOX.', $inbox2);
        self::assertNotSame($inbox1, $inbox2);

        $conn->close();
    }

    public function testCloseIsIdempotent(): void
    {
        $conn = Connection::connect(self::NATS_URL);
        $conn->close();
        $conn->close(); // Does not throw exception
        self::assertTrue($conn->isClosed());
    }

    public function testPublishAfterCloseThrows(): void
    {
        $conn = Connection::connect(self::NATS_URL);
        $conn->close();

        $this->expectException(\Nats\ConnectionClosedException::class);
        $conn->publish('test', 'data');
    }

    public function testJetStreamAvailable(): void
    {
        $conn = Connection::connect(self::NATS_URL);
        self::assertTrue($conn->jetStreamAvailable());
        $conn->close();
    }
}
