<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\Micro;

use Nats\Connection;
use Nats\Micro\EndpointConfig;
use Nats\Micro\Service;
use Nats\Micro\ServiceConfig;
use PHPUnit\Framework\TestCase;

final class GroupQueueTest extends TestCase
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

    public function testGroupWithQueueGroup(): void
    {
        $service = Service::create($this->conn, new ServiceConfig(
            name: 'group-queue-test',
            version: '1.0.0',
        ));

        $group = $service->addGroup('api', queueGroup: 'custom-queue');
        $group->addEndpoint('echo', function ($req): void {
            $req->respond($req->data());
        });

        $reply = $this->conn->request('api.echo', 'hello', 2.0);
        self::assertSame('hello', $reply->data);

        $service->stop();
    }

    public function testNestedGroupInheritsQueueGroup(): void
    {
        $service = Service::create($this->conn, new ServiceConfig(
            name: 'nested-queue-test',
            version: '1.0.0',
        ));

        $parent = $service->addGroup('v1', queueGroup: 'v1-queue');
        $child = $parent->addGroup('users');
        $child->addEndpoint('get', function ($req): void {
            $req->respond('user-data');
        });

        $reply = $this->conn->request('v1.users.get', '', 2.0);
        self::assertSame('user-data', $reply->data);

        $service->stop();
    }

    public function testGroupQueueGroupDisabled(): void
    {
        $service = Service::create($this->conn, new ServiceConfig(
            name: 'group-noqueue-test',
            version: '1.0.0',
        ));

        $group = $service->addGroup('broadcast', queueGroupDisabled: true);
        $group->addEndpoint('notify', function ($req): void {
            $req->respond('notified');
        });

        $reply = $this->conn->request('broadcast.notify', '', 2.0);
        self::assertSame('notified', $reply->data);

        $service->stop();
    }
}
