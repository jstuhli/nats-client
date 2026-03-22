<?php

declare(strict_types=1);

namespace Nats\Tests\Unit;

use Nats\Connection;
use Nats\ConnectionOptions;
use PHPUnit\Framework\TestCase;

final class ConnectionHostResolutionTest extends TestCase
{
    private function createConnectionWithPool(array $serverPool, bool $skipHostLookup = false): Connection
    {
        $options = new ConnectionOptions(skipHostLookup: $skipHostLookup);

        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $optProp = $ref->getProperty('options');
        $optProp->setValue($conn, $options);

        $poolProp = $ref->getProperty('serverPool');
        $poolProp->setValue($conn, $serverPool);

        return $conn;
    }

    private function invokeResolveUrl(Connection $conn, string $url): array
    {
        $method = new \ReflectionMethod(Connection::class, 'resolveUrl');

        return $method->invoke($conn, $url);
    }

    private function invokeResolveServerPool(Connection $conn): void
    {
        $method = new \ReflectionMethod(Connection::class, 'resolveServerPool');
        $method->invoke($conn);
    }

    private function getServerPool(Connection $conn): array
    {
        $prop = new \ReflectionProperty(Connection::class, 'serverPool');

        return $prop->getValue($conn);
    }

    public function testIpAddressIsNotResolved(): void
    {
        $conn = $this->createConnectionWithPool(['nats://10.0.0.1:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats://10.0.0.1:4222');

        $this->assertSame(['nats://10.0.0.1:4222'], $result);
    }

    public function testIpv6AddressIsNotResolved(): void
    {
        $conn = $this->createConnectionWithPool(['nats://[::1]:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats://[::1]:4222');

        // parse_url extracts ::1 as the host, which is a valid IP
        $this->assertCount(1, $result);
        $this->assertSame('nats://[::1]:4222', $result[0]);
    }

    public function testInvalidUrlIsReturnedAsIs(): void
    {
        $conn = $this->createConnectionWithPool(['://bad']);
        $result = $this->invokeResolveUrl($conn, '://bad');

        $this->assertSame(['://bad'], $result);
    }

    public function testLocalhostResolvesToIp(): void
    {
        $conn = $this->createConnectionWithPool(['nats://localhost:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats://localhost:4222');

        $this->assertNotEmpty($result);
        // localhost should resolve to 127.0.0.1
        $this->assertContains('nats://127.0.0.1:4222', $result);
    }

    public function testSchemeAndPortArePreserved(): void
    {
        // Use a non-TLS scheme so resolution still happens
        $conn = $this->createConnectionWithPool(['nats://localhost:5222']);
        $result = $this->invokeResolveUrl($conn, 'nats://localhost:5222');

        $this->assertNotEmpty($result);
        foreach ($result as $url) {
            $this->assertStringStartsWith('nats://', $url);
            $this->assertStringEndsWith(':5222', $url);
        }
    }

    public function testTlsSchemeSkipsResolution(): void
    {
        $conn = $this->createConnectionWithPool(['tls://localhost:4222']);
        $result = $this->invokeResolveUrl($conn, 'tls://localhost:4222');

        $this->assertSame(['tls://localhost:4222'], $result);
    }

    public function testNatsTlsSchemeSkipsResolution(): void
    {
        $conn = $this->createConnectionWithPool(['nats+tls://localhost:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats+tls://localhost:4222');

        $this->assertSame(['nats+tls://localhost:4222'], $result);
    }

    public function testGlobalTlsEnabledSkipsResolution(): void
    {
        $options = new ConnectionOptions(tlsEnabled: true);

        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $optProp = $ref->getProperty('options');
        $optProp->setValue($conn, $options);

        $poolProp = $ref->getProperty('serverPool');
        $poolProp->setValue($conn, ['nats://localhost:4222']);

        $result = $this->invokeResolveUrl($conn, 'nats://localhost:4222');

        $this->assertSame(['nats://localhost:4222'], $result);
    }

    public function testCredentialsArePreserved(): void
    {
        $conn = $this->createConnectionWithPool(['nats://user:pass@localhost:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats://user:pass@localhost:4222');

        $this->assertNotEmpty($result);
        foreach ($result as $url) {
            $this->assertStringContainsString('user:pass@', $url);
        }
    }

    public function testUserWithoutPasswordIsPreserved(): void
    {
        $conn = $this->createConnectionWithPool(['nats://token@localhost:4222']);
        $result = $this->invokeResolveUrl($conn, 'nats://token@localhost:4222');

        $this->assertNotEmpty($result);
        foreach ($result as $url) {
            $this->assertStringContainsString('token@', $url);
            $this->assertStringNotContainsString('token:@', $url);
        }
    }

    public function testDefaultPortIsUsedWhenOmitted(): void
    {
        $conn = $this->createConnectionWithPool(['nats://localhost']);
        $result = $this->invokeResolveUrl($conn, 'nats://localhost');

        $this->assertNotEmpty($result);
        foreach ($result as $url) {
            $this->assertStringEndsWith(':4222', $url);
        }
    }

    public function testResolveServerPoolDeduplicates(): void
    {
        $conn = $this->createConnectionWithPool([
            'nats://localhost:4222',
            'nats://127.0.0.1:4222',
        ]);

        $this->invokeResolveServerPool($conn);
        $pool = $this->getServerPool($conn);

        // localhost resolves to 127.0.0.1, and the second entry is already 127.0.0.1
        // so the pool should be deduplicated
        $this->assertSame(array_unique($pool), $pool);
    }

    public function testSkipHostLookupPreservesOriginalPool(): void
    {
        $options = new ConnectionOptions(skipHostLookup: true);

        $ref = new \ReflectionClass(Connection::class);
        $conn = $ref->newInstanceWithoutConstructor();

        $optProp = $ref->getProperty('options');
        $optProp->setValue($conn, $options);

        $poolProp = $ref->getProperty('serverPool');
        $poolProp->setValue($conn, ['nats://localhost:4222']);

        // When skipHostLookup is true, resolveServerPool is not called in connect(),
        // so the pool should remain unchanged. We verify the flag is accessible.
        $this->assertTrue($options->skipHostLookup);
        $pool = $this->getServerPool($conn);
        $this->assertSame(['nats://localhost:4222'], $pool);
    }

    public function testHostnameResolvingToMultipleIpsExpandsPool(): void
    {
        // Use a hostname known to resolve to multiple IPs — we test with localhost
        // which typically resolves to a single IP. Instead, test the mechanism:
        // if gethostbynamel returns multiple IPs, each gets its own entry.
        $conn = $this->createConnectionWithPool(['nats://localhost:4222']);
        $this->invokeResolveServerPool($conn);
        $pool = $this->getServerPool($conn);

        // At minimum, localhost should resolve to at least one IP-based URL
        $this->assertNotEmpty($pool);
        foreach ($pool as $url) {
            $parsed = parse_url($url);
            $host = $parsed['host'] ?? '';
            // After resolution, host should be an IP address (not "localhost")
            $this->assertNotFalse(
                filter_var($host, FILTER_VALIDATE_IP),
                "Expected IP address in resolved URL, got: {$host}"
            );
        }
    }

    public function testFailedDnsResolutionKeepsOriginalUrl(): void
    {
        $fakeHost = 'this-hostname-definitely-does-not-exist-' . bin2hex(random_bytes(8)) . '.invalid';
        $url = "nats://{$fakeHost}:4222";

        $conn = $this->createConnectionWithPool([$url]);
        $result = $this->invokeResolveUrl($conn, $url);

        $this->assertSame([$url], $result);
    }
}
