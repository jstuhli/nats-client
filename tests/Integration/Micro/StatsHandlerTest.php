<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\Micro;

use Nats\Connection;
use Nats\Micro\EndpointConfig;
use Nats\Micro\Service;
use Nats\Micro\ServiceConfig;
use PHPUnit\Framework\TestCase;

final class StatsHandlerTest extends TestCase
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

    public function testStatsHandlerInvoked(): void
    {
        $handlerCalled = false;
        $customData = ['custom_metric' => 42];

        $service = Service::create($this->conn, new ServiceConfig(
            name: 'stats-handler-test',
            version: '1.0.0',
            statsHandler: function ($endpointStats) use (&$handlerCalled, $customData) {
                $handlerCalled = true;
                return $customData;
            },
        ));

        $service->addEndpoint('echo', function ($req): void {
            $req->respond($req->data());
        });

        // Make a request to the endpoint
        $reply = $this->conn->request('echo', 'test', 2.0);
        self::assertSame('test', $reply->data);

        // Get stats — this should invoke the stats handler
        $stats = $service->stats();

        self::assertTrue($handlerCalled, 'Stats handler should have been invoked');
        self::assertNotEmpty($stats->endpoints);

        $epStats = $stats->endpoints[0];
        self::assertSame(1, $epStats->numRequests);
        self::assertSame(['custom_metric' => 42], $epStats->data);

        $service->stop();
    }

    public function testStatsWithoutHandlerHasNullData(): void
    {
        $service = Service::create($this->conn, new ServiceConfig(
            name: 'stats-no-handler',
            version: '1.0.0',
        ));

        $service->addEndpoint('ping', function ($req): void {
            $req->respond('pong');
        });

        $this->conn->request('ping', '', 2.0);

        $stats = $service->stats();
        $epStats = $stats->endpoints[0];
        self::assertNull($epStats->data);

        $service->stop();
    }
}
