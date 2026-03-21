<?php

declare(strict_types=1);

namespace Nats;

/**
 * @implements \IteratorAggregate<string, list<string>>
 */
final class Headers implements \IteratorAggregate, \Countable
{
    private const string VERSION_LINE = "NATS/1.0";
    private const string CRLF = "\r\n";

    /** @var array<string, list<string>> */
    private array $headers;

    /**
     * @param array<string, string|list<string>> $headers
     */
    public function __construct(array $headers = [])
    {
        $this->headers = [];
        foreach ($headers as $key => $value) {
            $this->headers[$key] = is_array($value) ? $value : [$value];
        }
    }

    public function get(string $key): ?string
    {
        return $this->headers[$key][0] ?? null;
    }

    /**
     * @return list<string>
     */
    public function values(string $key): array
    {
        return $this->headers[$key] ?? [];
    }

    public function set(string $key, string ...$values): self
    {
        $this->headers[$key] = array_values($values);
        return $this;
    }

    public function add(string $key, string $value): self
    {
        $this->headers[$key][] = $value;
        return $this;
    }

    public function has(string $key): bool
    {
        return isset($this->headers[$key]) && $this->headers[$key] !== [];
    }

    public function delete(string $key): self
    {
        unset($this->headers[$key]);
        return $this;
    }

    /**
     * @return array<string, list<string>>
     */
    public function all(): array
    {
        return $this->headers;
    }

    public function toWireFormat(?string $statusLine = null): string
    {
        $result = ($statusLine ?? self::VERSION_LINE) . self::CRLF;
        foreach ($this->headers as $key => $values) {
            foreach ($values as $value) {
                $result .= "{$key}: {$value}" . self::CRLF;
            }
        }
        $result .= self::CRLF;
        return $result;
    }

    public static function fromWireFormat(string $raw): self
    {
        $headers = new self();
        $lines = explode("\r\n", $raw);

        // Skip version line (NATS/1.0) and parse status if present
        $firstLine = array_shift($lines);
        if (str_starts_with($firstLine, 'NATS/1.0')) {
            $statusPart = trim(substr($firstLine, 8));
            if ($statusPart !== '') {
                $headers->set('Status', $statusPart);
            }
        }

        foreach ($lines as $line) {
            if ($line === '') {
                continue;
            }
            $colonPos = strpos($line, ':');
            if ($colonPos === false) {
                continue;
            }
            $key = substr($line, 0, $colonPos);
            $value = ltrim(substr($line, $colonPos + 1));
            $headers->add($key, $value);
        }

        return $headers;
    }

    /**
     * @return \ArrayIterator<string, list<string>>
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->headers);
    }

    public function count(): int
    {
        return count($this->headers);
    }
}
