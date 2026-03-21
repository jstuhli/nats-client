<?php

/**
 * Authentication - different methods.
 *
 * NATS supports: user/pass, token, NKey, JWT, credentials file.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\ConnectionOptions;

// 1. User/Password
$options = (new ConnectionOptions())
    ->userInfo('myuser', 'mypassword');

$conn = Connection::connect('nats://localhost:4222', $options);
$conn->close();

// 2. Token
$options = (new ConnectionOptions())
    ->token('s3cr3t-token');

$conn = Connection::connect('nats://localhost:4222', $options);
$conn->close();

// 3. Token from URL (embedded)
$conn = Connection::connect('nats://myuser:mypass@localhost:4222');
$conn->close();

// 4. NKey authentication (Ed25519)
// $options = (new ConnectionOptions())
//     ->nkey('SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY');
// $conn = Connection::connect('nats://localhost:4222', $options);

// 5. JWT + NKey
// $options = (new ConnectionOptions())
//     ->jwt($jwtString, $nkeySeed);

// 6. Credentials file (.creds)
// $options = (new ConnectionOptions())
//     ->credentials('/path/to/user.creds');

// 7. TLS
$options = (new ConnectionOptions())
    ->tls()
    ->tlsCertificate('/path/to/client-cert.pem', '/path/to/client-key.pem')
    ->tlsCaCertificate('/path/to/ca.pem');

// $conn = Connection::connect('tls://localhost:4222', $options);
