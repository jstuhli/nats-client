<?php

declare(strict_types=1);

namespace Nats\Enum;

enum RetentionPolicy: string
{
    case Limits = 'limits';
    case Interest = 'interest';
    case WorkQueue = 'workqueue';
}
