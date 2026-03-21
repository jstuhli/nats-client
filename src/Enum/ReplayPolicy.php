<?php

declare(strict_types=1);

namespace Nats\Enum;

enum ReplayPolicy: string
{
    case Instant = 'instant';
    case Original = 'original';
}
