<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\Micro;

use Nats\Connection;
use Nats\Message;
use Nats\Micro\EndpointConfig;
use Nats\Micro\RequestInterface;
use Nats\Micro\Service;
use Nats\Micro\ServiceConfig;
use PHPUnit\Framework\TestCase;

final class ServiceExtendedTest extends TestCase
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

    public function testServiceName(): void
    {
        $svcName = 'name-test-' . substr(uniqid(), -6);

        $svc = Service::create($this->conn, new ServiceConfig(
            name: $svcName,
            version: '1.0.0',
        ));

        self::assertSame($svcName, $svc->name());

        $svc->stop();
    }

    public function testServiceVersion(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'version-test-' . substr(uniqid(), -6),
            version: '2.5.3',
        ));

        self::assertSame('2.5.3', $svc->version());

        $svc->stop();
    }

    public function testServiceId(): void
    {
        $svc = Service::create($this->conn, new ServiceConfig(
            name: 'id-test-' . substr(uniqid(), -6),
            version: '1.0.0',
        ));

        $id = $svc->id();
        self::assertIsString($id);
        self::assertNotEmpty($id);

        // Create a second service and verify IDs are unique
        $svc2 = Service::create($this->conn, new ServiceConfig(
            name: 'id-test2-' . substr(uniqid(), -6),
            version: '1.0.0',
        ));

        self::assertNotSame($svc->id(), $svc2->id());

        $svc->stop();
        $svc2->stop();
    }

    public function testRequestMsg(): void
    {
        $svcName = 'reqmsg-test-' . substr(uniqid(), -6);
        $subject = 'svc.reqmsg.' . substr(uniqid(), -6);

        /** @var Message|null $capturedMsg */
        $capturedMsg = null;

        $svc = Service::create($this->conn, new ServiceConfig(
            name: $svcName,
            version: '1.0.0',
        ));

        $svc->addEndpoint('echo', function (RequestInterface $req) use (&$capturedMsg): void {
            // Call msg() to get the underlying Message
            $capturedMsg = $req->msg();

            // Respond with the data
            $req->respond('got: ' . $req->data());
        }, new EndpointConfig(subject: $subject));

        $reply = $this->conn->request($subject, 'hello-world', 2.0);

        self::assertSame('got: hello-world', $reply->data);

        // Verify that msg() returned a Message instance
        self::assertNotNull($capturedMsg, 'req->msg() should have returned a Message');
        self::assertInstanceOf(Message::class, $capturedMsg);
        self::assertSame('hello-world', $capturedMsg->data);
        self::assertSame($subject, $capturedMsg->subject);
        self::assertNotNull($capturedMsg->replyTo, 'Request message should have a replyTo subject');

        $svc->stop();
    }
}
