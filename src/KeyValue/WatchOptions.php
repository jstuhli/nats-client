<?php

declare(strict_types=1);

namespace Nats\KeyValue;

final class WatchOptions
{
    private function __construct(
        public readonly string $type,
        public readonly mixed $value = null,
    ) {}

    public static function includeHistory(): self
    {
        return new self('include_history');
    }

    public static function ignoreDeletes(): self
    {
        return new self('ignore_deletes');
    }

    public static function updatesOnly(): self
    {
        return new self('updates_only');
    }

    public static function metaOnly(): self
    {
        return new self('meta_only');
    }

    public static function resumeFromRevision(int $revision): self
    {
        return new self('resume_from_revision', $revision);
    }
}
