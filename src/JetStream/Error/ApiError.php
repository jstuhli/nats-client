<?php

declare(strict_types=1);

namespace Nats\JetStream\Error;

use Nats\Internal\TypeCast as T;

readonly class ApiError
{
    public function __construct(
        public int $code,
        public int $errCode,
        public string $description,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            code: T::int($data['code'] ?? 0),
            errCode: T::int($data['err_code'] ?? 0),
            description: T::string($data['description'] ?? 'Unknown error'),
        );
    }
}
