<?php

declare(strict_types=1);

namespace Nats\Protocol;

enum Command: string
{
    case Info = 'INFO';
    case Connect = 'CONNECT';
    case Pub = 'PUB';
    case HPub = 'HPUB';
    case Sub = 'SUB';
    case Unsub = 'UNSUB';
    case Msg = 'MSG';
    case HMsg = 'HMSG';
    case Ping = 'PING';
    case Pong = 'PONG';
    case Ok = '+OK';
    case Err = '-ERR';
}
