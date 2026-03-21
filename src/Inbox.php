<?php

declare(strict_types=1);

namespace Nats;

final class Inbox
{
    private const string DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
    private const int NUID_LENGTH = 22;

    public static function generate(string $prefix = '_INBOX'): string
    {
        return $prefix . '.' . self::nuid();
    }

    public static function nuid(): string
    {
        $result = '';
        $max = strlen(self::DIGITS) - 1;
        for ($i = 0; $i < self::NUID_LENGTH; $i++) {
            $result .= self::DIGITS[random_int(0, $max)];
        }
        return $result;
    }
}
