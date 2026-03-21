<?php

declare(strict_types=1);

namespace Nats\Enum;

enum ConnectionStatus: string
{
    case Disconnected = 'disconnected';
    case Connected = 'connected';
    case Closed = 'closed';
    case Reconnecting = 'reconnecting';
    case Connecting = 'connecting';
    case DrainingSubscriptions = 'draining_subs';
    case DrainingPublications = 'draining_pubs';
}
