<?php

declare(strict_types=1);

$finder = (new PhpCsFixer\Finder())
    ->in([
        __DIR__ . '/src',
        __DIR__ . '/tests',
    ])
    ->name('*.php')
;

return (new PhpCsFixer\Config())
    ->setRules([
        '@auto:risky' => true,
    ])
    ->setFinder($finder)
    ->setRiskyAllowed(true)
;
