<?php

declare(strict_types=1);

namespace Nats\Auth;

use Nats\NatsException;

readonly class NKeyAuthenticator implements AuthenticatorInterface
{
    /**
     * @param string $seed The NKey seed (starts with 'S')
     */
    public function __construct(
        private string $seed,
    ) {}

    /**
     * @return array<string, string>
     */
    public function buildConnectOptions(): array
    {
        $publicKey = $this->derivePublicKey();
        return ['nkey' => $publicKey];
    }

    public function sign(string $nonce): ?string
    {
        $rawSeed = self::decodeSeed($this->seed);
        if ($rawSeed === '') {
            throw new NatsException('Invalid NKey seed');
        }
        $keyPair = sodium_crypto_sign_seed_keypair($rawSeed);
        $secretKey = sodium_crypto_sign_secretkey($keyPair);
        $signature = sodium_crypto_sign_detached($nonce, $secretKey);

        sodium_memzero($rawSeed);
        sodium_memzero($secretKey);

        return self::base32Encode($signature);
    }

    private function derivePublicKey(): string
    {
        $rawSeed = self::decodeSeed($this->seed);
        if ($rawSeed === '') {
            throw new NatsException('Invalid NKey seed');
        }
        $keyPair = sodium_crypto_sign_seed_keypair($rawSeed);
        $publicKey = sodium_crypto_sign_publickey($keyPair);

        sodium_memzero($rawSeed);

        // Encode with user prefix byte (0 << 3 | 20 << 3 = prefix for user nkey)
        $prefixByte = chr(13 << 3); // User prefix
        $payload = $prefixByte . $publicKey;

        // CRC16
        $crc = self::crc16($payload);
        $payload .= pack('v', $crc);

        return self::base32Encode($payload);
    }

    private static function decodeSeed(string $seed): string
    {
        $decoded = self::base32Decode($seed);
        if (strlen($decoded) < 34) {
            throw new NatsException('Invalid NKey seed');
        }

        // Skip 2-byte prefix, take 32-byte seed
        $rawSeed = substr($decoded, 2, 32);

        // Verify CRC
        $payload = substr($decoded, 0, 34);
        $crcData = unpack('v', substr($decoded, 34, 2));
        if ($crcData === false) {
            throw new NatsException('Invalid NKey seed checksum');
        }
        $expectedCrc = $crcData[1];
        $actualCrc = self::crc16($payload);
        if ($expectedCrc !== $actualCrc) {
            throw new NatsException('Invalid NKey seed checksum');
        }


        return $rawSeed;
    }

    private static function base32Encode(string $data): string
    {
        $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
        $result = '';
        $buffer = 0;
        $bitsLeft = 0;

        for ($i = 0, $len = strlen($data); $i < $len; $i++) {
            $buffer = ($buffer << 8) | ord($data[$i]);
            $bitsLeft += 8;
            while ($bitsLeft >= 5) {
                $bitsLeft -= 5;
                $result .= $alphabet[($buffer >> $bitsLeft) & 0x1f];
            }
        }

        if ($bitsLeft > 0) {
            $result .= $alphabet[($buffer << (5 - $bitsLeft)) & 0x1f];
        }

        return $result;
    }

    private static function base32Decode(string $data): string
    {
        $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
        $result = '';
        $buffer = 0;
        $bitsLeft = 0;

        for ($i = 0, $len = strlen($data); $i < $len; $i++) {
            $val = strpos($alphabet, $data[$i]);
            if ($val === false) {
                continue;
            }
            $buffer = ($buffer << 5) | $val;
            $bitsLeft += 5;
            if ($bitsLeft >= 8) {
                $bitsLeft -= 8;
                $result .= chr(($buffer >> $bitsLeft) & 0xff);
            }
        }

        return $result;
    }

    private static function crc16(string $data): int
    {
        $crc = 0;
        for ($i = 0, $len = strlen($data); $i < $len; $i++) {
            $crc = ($crc >> 8) ^ self::CRC16_TABLE[($crc ^ ord($data[$i])) & 0xff];
        }
        return $crc;
    }

    /** @var list<int> */
    private const array CRC16_TABLE = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x4864, 0x5845, 0x6826, 0x7807, 0x08e0, 0x18c1, 0x28a2, 0x38a3,
        0xc94c, 0xd96d, 0xe90e, 0xf92f, 0x89c8, 0x99e9, 0xa98a, 0xb9ab,
        0x5a75, 0x4a54, 0x7a37, 0x6a16, 0x1af1, 0x0ad0, 0x3ab3, 0x2a92,
        0xdb7d, 0xcb5c, 0xfb3f, 0xeb1e, 0x9bf9, 0x8bd8, 0xbbbb, 0xab9a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
    ];
}
