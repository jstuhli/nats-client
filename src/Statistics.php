<?php

declare(strict_types=1);

namespace Nats;

readonly class Statistics
{
    public function __construct(
        public int $inMsgs = 0,
        public int $outMsgs = 0,
        public int $inBytes = 0,
        public int $outBytes = 0,
        public int $reconnects = 0,
    ) {}
}
