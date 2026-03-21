<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\BadSubjectException;
use Nats\Connection;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class SubjectValidationTest extends TestCase
{
    #[DataProvider('validSubjects')]
    public function testAcceptsValidSubjects(string $subject): void
    {
        // Should not throw
        Connection::validateSubject($subject);
        $this->addToAssertionCount(1);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function validSubjects(): iterable
    {
        yield 'simple' => ['foo'];
        yield 'dotted' => ['foo.bar'];
        yield 'multi-level' => ['foo.bar.baz'];
        yield 'wildcard-star' => ['foo.*'];
        yield 'wildcard-gt' => ['foo.>'];
        yield 'with-underscore' => ['_INBOX.abc123'];
        yield 'with-dash' => ['my-subject'];
        yield 'dollar-prefix' => ['$JS.API.STREAM.LIST'];
        yield 'kv-subject' => ['$KV.mybucket.mykey'];
        yield 'object-store' => ['$O.mybucket.M.myfile'];
        yield 'single-char' => ['x'];
        yield 'numeric' => ['123'];
        yield 'mixed' => ['a1.b2.c3'];
    }

    #[DataProvider('invalidSubjects')]
    public function testRejectsInvalidSubjects(string $subject): void
    {
        $this->expectException(BadSubjectException::class);
        Connection::validateSubject($subject);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function invalidSubjects(): iterable
    {
        yield 'empty' => [''];
        yield 'with-space' => ['foo bar'];
        yield 'with-tab' => ["foo\tbar"];
        yield 'with-cr' => ["foo\rbar"];
        yield 'with-lf' => ["foo\nbar"];
        yield 'with-null-byte' => ["foo\x00bar"];
        yield 'only-space' => [' '];
        yield 'leading-space' => [' foo'];
        yield 'trailing-space' => ['foo '];
        yield 'with-crlf' => ["foo\r\nbar"];
        yield 'with-bell' => ["foo\x07bar"];
        yield 'with-escape' => ["foo\x1bbar"];
        yield 'with-del' => ["foo\x7fbar"];
    }
}
