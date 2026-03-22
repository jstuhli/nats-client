<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

require_once __DIR__ . '/PsrLoggerStub.php';

use Nats\ConnectionOptions;
use PHPUnit\Framework\TestCase;

final class ConnectionOptionsTest extends TestCase
{
    public function testLoggerRequiresPsrLoggerInterface(): void
    {
        $opts = new ConnectionOptions();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Logger must implement Psr\\Log\\LoggerInterface');

        $opts->withLogger(new class {});
    }

    public function testLoggerAcceptsPsrLoggerInterface(): void
    {
        $opts = new ConnectionOptions();
        $logger = new class implements \Psr\Log\LoggerInterface {
            public function emergency(string|\Stringable $message, array $context = []): void {}
            public function alert(string|\Stringable $message, array $context = []): void {}
            public function critical(string|\Stringable $message, array $context = []): void {}
            public function error(string|\Stringable $message, array $context = []): void {}
            public function warning(string|\Stringable $message, array $context = []): void {}
            public function notice(string|\Stringable $message, array $context = []): void {}
            public function info(string|\Stringable $message, array $context = []): void {}
            public function debug(string|\Stringable $message, array $context = []): void {}
            public function log(mixed $level, string|\Stringable $message, array $context = []): void {}
        };

        $opts = $opts->withLogger($logger);

        self::assertSame($logger, $opts->logger);
    }

    public function testNoCallbacksAfterClientClose(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->noCallbacksAfterClientClose);

        $opts = $opts->withNoCallbacksAfterClientClose();
        self::assertTrue($opts->noCallbacksAfterClientClose);
    }

    public function testSkipHostLookup(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->skipHostLookup);

        $opts = $opts->withSkipHostLookup();
        self::assertTrue($opts->skipHostLookup);
    }

    public function testSkipSubjectValidation(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->skipSubjectValidation);

        $opts = $opts->withSkipSubjectValidation();
        self::assertTrue($opts->skipSubjectValidation);
    }

    public function testCustomReconnectDelay(): void
    {
        $opts = new ConnectionOptions();
        self::assertNull($opts->customReconnectDelay);

        $cb = static fn(int $attempt): float => $attempt * 0.5;
        $opts = $opts->withCustomReconnectDelay($cb);

        $result = $opts->customReconnectDelay;
        self::assertNotNull($result);
        self::assertSame(1.5, $result(3));
    }

    public function testSyncQueueLen(): void
    {
        $opts = new ConnectionOptions();
        self::assertSame(65536, $opts->syncQueueLen);

        $opts = $opts->withSyncQueueLen(1024);
        self::assertSame(1024, $opts->syncQueueLen);
    }

    public function testPermissionErrOnSubscribe(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->permissionErrOnSubscribe);

        $opts = $opts->withPermissionErrOnSubscribe();
        self::assertTrue($opts->permissionErrOnSubscribe);
    }

    public function testDefaultValues(): void
    {
        $opts = new ConnectionOptions();

        // New options defaults
        self::assertFalse($opts->noCallbacksAfterClientClose);
        self::assertFalse($opts->skipHostLookup);
        self::assertFalse($opts->skipSubjectValidation);
        self::assertNull($opts->customReconnectDelay);
        self::assertSame(65536, $opts->syncQueueLen);
        self::assertFalse($opts->permissionErrOnSubscribe);

        // Existing options defaults
        self::assertSame(['nats://127.0.0.1:4222'], $opts->servers);
        self::assertNull($opts->name);
        self::assertNull($opts->authenticator);
        self::assertFalse($opts->tlsEnabled);
        self::assertSame(60, $opts->maxReconnects);
        self::assertSame(2.0, $opts->reconnectWait);
        self::assertFalse($opts->noReconnect);
        self::assertFalse($opts->dontRandomize);
        self::assertSame(2.0, $opts->timeout);
        self::assertSame(120.0, $opts->pingInterval);
        self::assertSame(2, $opts->maxPingsOutstanding);
        self::assertSame(30.0, $opts->drainTimeout);
        self::assertNull($opts->onConnect);
        self::assertNull($opts->onDisconnect);
        self::assertNull($opts->onReconnect);
        self::assertNull($opts->onClose);
        self::assertNull($opts->onError);
        self::assertFalse($opts->noEcho);
        self::assertFalse($opts->verbose);
        self::assertFalse($opts->pedantic);
        self::assertFalse($opts->ignoreDiscoveredServers);
        self::assertSame('_INBOX', $opts->inboxPrefix);
        self::assertFalse($opts->compression);
        self::assertNull($opts->logger);
    }

    public function testWithersReturnNewInstance(): void
    {
        $opts = new ConnectionOptions();
        $opts2 = $opts->withName('test');

        self::assertNull($opts->name);
        self::assertSame('test', $opts2->name);
        self::assertNotSame($opts, $opts2);
    }

    public function testConstructorWithNamedArguments(): void
    {
        $opts = new ConnectionOptions(
            name: 'my-app',
            timeout: 5.0,
            maxReconnects: 10,
            noEcho: true,
            inboxPrefix: '_MY_INBOX',
        );

        self::assertSame('my-app', $opts->name);
        self::assertSame(5.0, $opts->timeout);
        self::assertSame(10, $opts->maxReconnects);
        self::assertTrue($opts->noEcho);
        self::assertSame('_MY_INBOX', $opts->inboxPrefix);

        // Non-specified values keep defaults
        self::assertSame(['nats://127.0.0.1:4222'], $opts->servers);
        self::assertSame(2.0, $opts->reconnectWait);
        self::assertFalse($opts->verbose);
    }
}
