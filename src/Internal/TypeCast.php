<?php

declare(strict_types=1);

namespace Nats\Internal;

/**
 * Type-safe casting helpers for mixed values from JSON-decoded data.
 *
 * @internal
 */
final class TypeCast
{
    public static function string(mixed $value, string $default = ''): string
    {
        if (is_string($value)) {
            return $value;
        }
        if (is_scalar($value)) {
            return (string) $value;
        }
        return $default;
    }

    public static function int(mixed $value, int $default = 0): int
    {
        if (is_int($value)) {
            return $value;
        }
        if (is_scalar($value)) {
            return (int) $value;
        }
        return $default;
    }

    public static function float(mixed $value, float $default = 0.0): float
    {
        if (is_float($value) || is_int($value)) {
            return (float) $value;
        }
        if (is_scalar($value)) {
            return (float) $value;
        }
        return $default;
    }

    public static function bool(mixed $value, bool $default = false): bool
    {
        if (is_bool($value)) {
            return $value;
        }
        if (is_scalar($value)) {
            return (bool) $value;
        }
        return $default;
    }

    public static function nullableString(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }
        if (is_string($value)) {
            return $value;
        }
        if (is_scalar($value)) {
            return (string) $value;
        }
        return null;
    }

    public static function nullableInt(mixed $value): ?int
    {
        if ($value === null) {
            return null;
        }
        if (is_int($value)) {
            return $value;
        }
        if (is_scalar($value)) {
            return (int) $value;
        }
        return null;
    }

    /**
     * @return array<string, mixed>
     */
    public static function stringKeyArray(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }
        $result = [];
        foreach ($value as $k => $v) {
            $result[(string) $k] = $v;
        }
        return $result;
    }

    /**
     * @return list<mixed>
     */
    public static function list(mixed $value): array
    {
        return is_array($value) ? array_values($value) : [];
    }

    /**
     * @return array<string, string>
     */
    public static function stringMap(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }
        $result = [];
        foreach ($value as $k => $v) {
            $result[(string) $k] = is_string($v) ? $v : (is_scalar($v) ? (string) $v : '');
        }
        return $result;
    }
}
