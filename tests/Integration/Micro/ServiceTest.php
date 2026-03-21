<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\Micro;

use Nats\Connection;
use Nats\Micro\EndpointConfig;
use Nats\Micro\HandlerInterface;
use Nats\Micro\RequestInterface;
use Nats\Micro\Service;
use Nats\Micro\ServiceConfig;
use PHPUnit\Framework\TestCase;

final class ServiceTest extends TestCase
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

    public function testCreateService(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'test-svc',
            version: '1.0.0',
            description: 'Test service',
        ));

        self::assertFalse($svc->isStopped());
        self::assertNotEmpty($svc->id());

        $svc->stop();
        self::assertTrue($svc->isStopped());
    }

    public function testAddEndpointClosure(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'test-calc',
            version: '1.0.0',
        ));

        $svc->addEndpoint('add', function (RequestInterface $req): void {
            $data = json_decode($req->data(), true);
            $result = ($data['a'] ?? 0) + ($data['b'] ?? 0);
            $req->respondJson(['result' => $result]);
        }, new EndpointConfig(subject: 'svc.calc.add'));

        // Call the endpoint
        $reply = $this->conn->request('svc.calc.add', json_encode(['a' => 3, 'b' => 7]), 2.0);
        $result = json_decode($reply->data, true);

        self::assertSame(10, $result['result']);

        $svc->stop();
    }

    public function testAddEndpointHandler(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'test-handler',
            version: '1.0.0',
        ));

        $handler = new class implements HandlerInterface {
            public function handle(RequestInterface $request): void
            {
                $request->respond('PONG: ' . $request->data());
            }
        };

        $svc->addEndpoint('ping', $handler, new EndpointConfig(subject: 'svc.test.ping'));

        $reply = $this->conn->request('svc.test.ping', 'hello', 2.0);
        self::assertSame('PONG: hello', $reply->data);

        $svc->stop();
    }

    public function testErrorResponse(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'test-error',
            version: '1.0.0',
        ));

        $svc->addEndpoint('fail', function (RequestInterface $req): void {
            $req->error('400', 'Bad input', 'details here');
        }, new EndpointConfig(subject: 'svc.test.fail'));

        $reply = $this->conn->request('svc.test.fail', 'bad data', 2.0);

        self::assertSame('details here', $reply->data);
        self::assertTrue($reply->hasHeaders());
        self::assertSame('Bad input', $reply->headers->get('Nats-Service-Error'));
        self::assertSame('400', $reply->headers->get('Nats-Service-Error-Code'));

        $svc->stop();
    }

    public function testServiceDiscoveryPing(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'discoverable',
            version: '2.0.0',
            metadata: ['region' => 'eu'],
        ));

        // Ping all services
        $reply = $this->conn->request('$SRV.PING', '', 2.0);
        $data = json_decode($reply->data, true);

        self::assertSame('discoverable', $data['name']);
        self::assertSame('2.0.0', $data['version']);

        // Ping by name
        $reply = $this->conn->request('$SRV.PING.discoverable', '', 2.0);
        $data = json_decode($reply->data, true);
        self::assertSame('discoverable', $data['name']);

        // Ping by name + id
        $id = $svc->id();
        $reply = $this->conn->request("\$SRV.PING.discoverable.{$id}", '', 2.0);
        $data = json_decode($reply->data, true);
        self::assertSame($id, $data['id']);

        $svc->stop();
    }

    public function testServiceDiscoveryInfo(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'info-svc',
            version: '1.0.0',
            description: 'Info test',
        ));

        $svc->addEndpoint('greet', function (RequestInterface $req): void {
            $req->respond('hi');
        }, new EndpointConfig(subject: 'svc.info.greet'));

        $reply = $this->conn->request('$SRV.INFO.info-svc', '', 2.0);
        $data = json_decode($reply->data, true);

        self::assertSame('info-svc', $data['name']);
        self::assertSame('1.0.0', $data['version']);
        self::assertSame('Info test', $data['description']);
        self::assertNotEmpty($data['endpoints']);
        self::assertSame('greet', $data['endpoints'][0]['name']);

        $svc->stop();
    }

    public function testServiceDiscoveryStats(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'stats-svc',
            version: '1.0.0',
        ));

        $svc->addEndpoint('echo', function (RequestInterface $req): void {
            $req->respond($req->data());
        }, new EndpointConfig(subject: 'svc.stats.echo'));

        // Make some calls
        for ($i = 0; $i < 5; $i++) {
            $this->conn->request('svc.stats.echo', "msg{$i}", 2.0);
        }

        $reply = $this->conn->request('$SRV.STATS.stats-svc', '', 2.0);
        $data = json_decode($reply->data, true);

        self::assertSame('stats-svc', $data['name']);
        self::assertNotEmpty($data['endpoints']);

        $epStats = $data['endpoints'][0];
        self::assertSame('echo', $epStats['name']);
        self::assertSame(5, $epStats['num_requests']);
        self::assertSame(0, $epStats['num_errors']);
        self::assertGreaterThan(0, $epStats['processing_time']);

        $svc->stop();
    }

    public function testServiceInfo(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'prog-info',
            version: '3.0.0',
            metadata: ['team' => 'backend'],
        ));

        $svc->addEndpoint('ep1', function (RequestInterface $req): void {
            $req->respond('ok');
        }, new EndpointConfig(subject: 'svc.prog.ep1'));

        $info = $svc->info();
        self::assertSame('prog-info', $info->identity->name);
        self::assertSame('3.0.0', $info->identity->version);
        self::assertNotEmpty($info->identity->id);
        self::assertCount(1, $info->endpoints);

        $svc->stop();
    }

    public function testServiceStats(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'prog-stats',
            version: '1.0.0',
        ));

        $svc->addEndpoint('counter', function (RequestInterface $req): void {
            $req->respond('counted');
        }, new EndpointConfig(subject: 'svc.prog.counter'));

        $this->conn->request('svc.prog.counter', 'test', 2.0);

        $stats = $svc->stats();
        self::assertSame('prog-stats', $stats->identity->name);
        self::assertCount(1, $stats->endpoints);
        self::assertSame(1, $stats->endpoints[0]->numRequests);

        $svc->stop();
    }

    public function testServiceReset(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'reset-svc',
            version: '1.0.0',
        ));

        $svc->addEndpoint('ep', function (RequestInterface $req): void {
            $req->respond('ok');
        }, new EndpointConfig(subject: 'svc.reset.ep'));

        $this->conn->request('svc.reset.ep', 'test', 2.0);
        $this->conn->request('svc.reset.ep', 'test', 2.0);

        $stats = $svc->stats();
        self::assertSame(2, $stats->endpoints[0]->numRequests);

        $svc->reset();

        $stats = $svc->stats();
        self::assertSame(0, $stats->endpoints[0]->numRequests);

        $svc->stop();
    }

    public function testServiceGroup(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'group-svc',
            version: '1.0.0',
        ));

        $apiGroup = $svc->addGroup('api');
        $apiGroup->addEndpoint('users', function (RequestInterface $req): void {
            $req->respondJson(['users' => ['alice', 'bob']]);
        });

        $reply = $this->conn->request('api.users', '', 2.0);
        $data = json_decode($reply->data, true);
        self::assertSame(['alice', 'bob'], $data['users']);

        $svc->stop();
    }

    public function testDoneHandler(): void
    {
        $doneCalled = false;

        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'done-svc',
            version: '1.0.0',
            doneHandler: function () use (&$doneCalled): void {
                $doneCalled = true;
            },
        ));

        $svc->stop();
        self::assertTrue($doneCalled);
    }

    public function testStopIsIdempotent(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'idempotent-svc',
            version: '1.0.0',
        ));

        $svc->stop();
        $svc->stop(); // Should not throw
        self::assertTrue($svc->isStopped());
    }
}
