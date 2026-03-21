<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Inbox;
use PHPUnit\Framework\TestCase;

final class InboxTest extends TestCase
{
    public function testGenerate(): void
    {
        $inbox = Inbox::generate();
        self::assertStringStartsWith('_INBOX.', $inbox);
        self::assertSame(7 + 22, strlen($inbox)); // _INBOX. + 22 chars
    }

    public function testGenerateWithCustomPrefix(): void
    {
        $inbox = Inbox::generate('MYPREFIX');
        self::assertStringStartsWith('MYPREFIX.', $inbox);
    }

    public function testUniqueness(): void
    {
        $inbox1 = Inbox::generate();
        $inbox2 = Inbox::generate();
        self::assertNotSame($inbox1, $inbox2);
    }

    public function testNuid(): void
    {
        $nuid = Inbox::nuid();
        self::assertSame(22, strlen($nuid));
        self::assertMatchesRegularExpression('/^[0-9A-Za-z]+$/', $nuid);
    }
}
