<?php

declare(strict_types=1);

namespace Nats\Enum;

enum ServiceVerb: string
{
    case Ping = 'PING';
    case Stats = 'STATS';
    case Info = 'INFO';
}
