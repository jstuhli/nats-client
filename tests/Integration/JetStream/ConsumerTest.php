<?php

declare(strict_types=1);

namespace Nats\Tests\Integration\JetStream;

use Nats\Connection;
use Nats\Enum\AckPolicy;
use Nats\Enum\DeliveryPolicy;
use Nats\Enum\StorageType;
use Nats\JetStream\Consumer\ConsumerConfig;
use Nats\JetStream\Consumer\ConsumeOptions;
use Nats\JetStream\Consumer\FetchOptions;
use Nats\JetStream\Consumer\OrderedConsumerConfig;
use Nats\JetStream\JetStreamContext;
use Nats\JetStream\Stream\StreamConfig;
use Nats\JetStream\Stream\StreamInterface;
use PHPUnit\Framework\TestCase;

final class ConsumerTest extends TestCase
{
    private const NATS_URL = 'nats://127.0.0.1:4222';
    private Connection $conn;
    private JetStreamContext $js;
    private StreamInterface $stream;

    protected function setUp(): void
    {
        $this->conn = Connection::connect(self::NATS_URL);
        $this->js = $this->conn->jetStream();
        try { $this->js->deleteStream('TEST_CONSUMER'); } catch (\Throwable) {}

        $this->stream = $this->js->createStream(new StreamConfig(
            name: 'TEST_CONSUMER',
            subjects: ['test.cons.>'],
            storage: StorageType::Memory,
        ));

        // Seed messages
        for ($i = 1; $i <= 20; $i++) {
            $this->js->publish('test.cons.events', json_encode(['id' => $i]));
        }
    }

    protected function tearDown(): void
    {
        try { $this->js->deleteStream('TEST_CONSUMER'); } catch (\Throwable) {}
        $this->conn->close();
    }

    public function testCreateConsumer(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-consumer-1',
            durable: 'test-consumer-1',
            ackPolicy: AckPolicy::Explicit,
            deliverPolicy: DeliveryPolicy::All,
        ));

        $info = $consumer->info();
        self::assertSame('TEST_CONSUMER', $info->stream);
        self::assertSame('test-consumer-1', $info->name);
        self::assertSame(20, $info->numPending);
    }

    public function testFetch(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-fetch',
            durable: 'test-fetch',
            ackPolicy: AckPolicy::Explicit,
        ));

        $batch = $consumer->fetch(5, new FetchOptions(timeout: 3.0));
        $messages = [];
        foreach ($batch as $msg) {
            $messages[] = json_decode($msg->data(), true);
            $msg->ack();
        }

        self::assertCount(5, $messages);
        self::assertSame(1, $messages[0]['id']);
        self::assertSame(5, $messages[4]['id']);
    }

    public function testFetchNoWait(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-nowait',
            durable: 'test-nowait',
            ackPolicy: AckPolicy::Explicit,
        ));

        $batch = $consumer->fetchNoWait(10);
        $count = 0;
        foreach ($batch as $msg) {
            $count++;
            $msg->ack();
        }

        self::assertGreaterThan(0, $count);
        self::assertLessThanOrEqual(10, $count);
    }

    public function testNext(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-next',
            durable: 'test-next',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msg = $consumer->next(3.0);
        $data = json_decode($msg->data(), true);
        self::assertSame(1, $data['id']);
        $msg->ack();
    }

    public function testMessageMetadata(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-meta',
            durable: 'test-meta',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msg = $consumer->next(3.0);
        $meta = $msg->metadata();

        self::assertGreaterThan(0, $meta->streamSequence);
        self::assertGreaterThan(0, $meta->consumerSequence);
        self::assertSame('TEST_CONSUMER', $meta->stream);
        self::assertSame('test-meta', $meta->consumer);

        $msg->ack();
    }

    public function testAckVariants(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-ack',
            durable: 'test-ack',
            ackPolicy: AckPolicy::Explicit,
            maxDeliver: 5,
        ));

        // Regular ack
        $msg = $consumer->next(3.0);
        $msg->ack();

        // Nak (will redeliver)
        $msg = $consumer->next(3.0);
        $msg->nak();

        // InProgress then ack
        $msg = $consumer->next(3.0);
        $msg->inProgress();
        $msg->ack();

        // Term (no more redelivery)
        $msg = $consumer->next(3.0);
        $msg->term();

        self::assertTrue(true); // No exceptions = success
    }

    public function testMessagesIterator(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-iter',
            durable: 'test-iter',
            ackPolicy: AckPolicy::Explicit,
        ));

        $msgs = $consumer->messages(new ConsumeOptions(
            maxMessages: 10,
            expires: 2.0,
        ));

        $count = 0;
        foreach ($msgs as $msg) {
            $msg->ack();
            $count++;
            if ($count >= 5) {
                $msgs->stop();
            }
        }

        self::assertSame(5, $count);
    }

    public function testOrderedConsumer(): void
    {
        $consumer = $this->stream->orderedConsumer(new OrderedConsumerConfig(
            deliverPolicy: DeliveryPolicy::All,
        ));

        $batch = $consumer->fetch(5, new FetchOptions(timeout: 3.0));
        $ids = [];
        foreach ($batch as $msg) {
            $ids[] = json_decode($msg->data(), true)['id'];
        }

        self::assertCount(5, $ids);
        // Ordered consumer guarantees order
        for ($i = 0; $i < count($ids) - 1; $i++) {
            self::assertLessThan($ids[$i + 1], $ids[$i]);
        }
    }

    public function testFilterSubject(): void
    {
        // Add some messages with different subjects
        $this->js->publish('test.cons.orders', '{"type":"order"}');
        $this->js->publish('test.cons.payments', '{"type":"payment"}');
        $this->js->publish('test.cons.orders', '{"type":"order2"}');

        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-filter',
            durable: 'test-filter',
            ackPolicy: AckPolicy::Explicit,
            filterSubject: 'test.cons.orders',
        ));

        $batch = $consumer->fetch(10, new FetchOptions(timeout: 2.0));
        $subjects = [];
        foreach ($batch as $msg) {
            $subjects[] = $msg->subject();
            $msg->ack();
        }

        foreach ($subjects as $subject) {
            self::assertSame('test.cons.orders', $subject);
        }
    }

    public function testConsumerInfo(): void
    {
        $consumer = $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'test-info',
            durable: 'test-info',
            ackPolicy: AckPolicy::Explicit,
            description: 'Test consumer',
        ));

        $info = $consumer->info();
        self::assertSame('TEST_CONSUMER', $info->stream);
        self::assertSame('test-info', $info->name);
        self::assertSame(20, $info->numPending);
        self::assertSame(0, $info->numAckPending);
    }

    public function testListConsumers(): void
    {
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'list-a', durable: 'list-a',
        ));
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'list-b', durable: 'list-b',
        ));

        $names = [];
        foreach ($this->stream->consumerNames() as $name) {
            $names[] = $name;
        }

        self::assertContains('list-a', $names);
        self::assertContains('list-b', $names);
    }

    public function testDeleteConsumer(): void
    {
        $this->stream->createOrUpdateConsumer(new ConsumerConfig(
            name: 'to-delete', durable: 'to-delete',
        ));

        $this->stream->deleteConsumer('to-delete');

        $this->expectException(\Nats\JetStream\Error\JetStreamException::class);
        $this->stream->consumer('to-delete');
    }
}
