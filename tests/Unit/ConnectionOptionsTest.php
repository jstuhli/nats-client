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

        $opts->logger(new class {});
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

        $opts->logger($logger);

        self::assertSame($logger, $opts->getLogger());
    }

    public function testNoCallbacksAfterClientClose(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->isNoCallbacksAfterClientClose());

        $opts->noCallbacksAfterClientClose();
        self::assertTrue($opts->isNoCallbacksAfterClientClose());
    }

    public function testSkipHostLookup(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->isSkipHostLookup());

        $opts->skipHostLookup();
        self::assertTrue($opts->isSkipHostLookup());
    }

    public function testSkipSubjectValidation(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->isSkipSubjectValidation());

        $opts->skipSubjectValidation();
        self::assertTrue($opts->isSkipSubjectValidation());
    }

    public function testCustomReconnectDelay(): void
    {
        $opts = new ConnectionOptions();
        self::assertNull($opts->getCustomReconnectDelay());

        $cb = static fn(int $attempt): float => $attempt * 0.5;
        $opts->customReconnectDelay($cb);

        $result = $opts->getCustomReconnectDelay();
        self::assertNotNull($result);
        self::assertSame(1.5, $result(3));
    }

    public function testSyncQueueLen(): void
    {
        $opts = new ConnectionOptions();
        self::assertSame(65536, $opts->getSyncQueueLen());

        $opts->syncQueueLen(1024);
        self::assertSame(1024, $opts->getSyncQueueLen());
    }

    public function testPermissionErrOnSubscribe(): void
    {
        $opts = new ConnectionOptions();
        self::assertFalse($opts->isPermissionErrOnSubscribe());

        $opts->permissionErrOnSubscribe();
        self::assertTrue($opts->isPermissionErrOnSubscribe());
    }

    public function testDefaultValues(): void
    {
        $opts = new ConnectionOptions();

        // New options defaults
        self::assertFalse($opts->isNoCallbacksAfterClientClose());
        self::assertFalse($opts->isSkipHostLookup());
        self::assertFalse($opts->isSkipSubjectValidation());
        self::assertNull($opts->getCustomReconnectDelay());
        self::assertSame(65536, $opts->getSyncQueueLen());
        self::assertFalse($opts->isPermissionErrOnSubscribe());

        // Existing options defaults
        self::assertSame(['nats://127.0.0.1:4222'], $opts->getServers());
        self::assertNull($opts->getName());
        self::assertNull($opts->getAuthenticator());
        self::assertFalse($opts->isTlsEnabled());
        self::assertSame(60, $opts->getMaxReconnects());
        self::assertSame(2.0, $opts->getReconnectWait());
        self::assertFalse($opts->isNoReconnect());
        self::assertFalse($opts->isDontRandomize());
        self::assertSame(2.0, $opts->getTimeout());
        self::assertSame(120.0, $opts->getPingInterval());
        self::assertSame(2, $opts->getMaxPingsOutstanding());
        self::assertSame(30.0, $opts->getDrainTimeout());
        self::assertNull($opts->getOnConnect());
        self::assertNull($opts->getOnDisconnect());
        self::assertNull($opts->getOnReconnect());
        self::assertNull($opts->getOnClose());
        self::assertNull($opts->getOnError());
        self::assertFalse($opts->isNoEcho());
        self::assertFalse($opts->isVerbose());
        self::assertFalse($opts->isPedantic());
        self::assertFalse($opts->isIgnoreDiscoveredServers());
        self::assertSame('_INBOX', $opts->getInboxPrefix());
        self::assertFalse($opts->isCompression());
        self::assertNull($opts->getLogger());
    }
}
