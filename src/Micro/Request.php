<?php

declare(strict_types=1);

namespace Nats\Micro;

use Nats\Connection;
use Nats\Headers;
use Nats\Message;
use Nats\NatsException;

final class Request implements RequestInterface
{
    public function __construct(
        private readonly Message $msg,
        private readonly Connection $connection,
    ) {}

    public function respond(string $data, ?Headers $headers = null): void
    {
        if ($this->msg->replyTo === null) {
            throw new NatsException('No reply subject');
        }

        $response = new Message(
            subject: $this->msg->replyTo,
            data: $data,
            headers: $headers,
        );
        $this->connection->publishMessage($response);
    }

    public function respondJson(mixed $data, ?Headers $headers = null): void
    {
        $headers ??= new Headers();
        $headers->set('Content-Type', 'application/json');
        $this->respond(json_encode($data, JSON_THROW_ON_ERROR), $headers);
    }

    public function error(string $code, string $description, string $data = ''): void
    {
        if ($this->msg->replyTo === null) {
            throw new NatsException('No reply subject');
        }

        $headers = new Headers();
        $headers->set('Nats-Service-Error', $description);
        $headers->set('Nats-Service-Error-Code', $code);

        $response = new Message(
            subject: $this->msg->replyTo,
            data: $data,
            headers: $headers,
        );
        $this->connection->publishMessage($response);
    }

    public function data(): string
    {
        return $this->msg->data;
    }

    public function headers(): ?Headers
    {
        return $this->msg->headers;
    }

    public function subject(): string
    {
        return $this->msg->subject;
    }

    public function replyTo(): ?string
    {
        return $this->msg->replyTo;
    }

    public function msg(): Message
    {
        return $this->msg;
    }
}
