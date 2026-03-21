<?php

declare(strict_types=1);

namespace Nats\Enum;

enum StorageType: string
{
    case File = 'file';
    case Memory = 'memory';
}
