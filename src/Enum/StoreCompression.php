<?php

declare(strict_types=1);

namespace Nats\Enum;

enum StoreCompression: string
{
    case None = 'none';
    case S2 = 's2';
}
