<?php

/**
 * Micro Service API - microservices framework built on NATS.
 *
 * Automatically registers discovery subjects ($SRV.PING/INFO/STATS),
 * supports endpoint grouping, queue groups, statistics.
 */

require_once __DIR__ . '/../../../vendor/autoload.php';

use Nats\Connection;
use Nats\Micro\EndpointConfig;
use Nats\Micro\Group;
use Nats\Micro\HandlerInterface;
use Nats\Micro\RequestInterface;
use Nats\Micro\Service;
use Nats\Micro\ServiceConfig;

$conn = Connection::connect();

// --- Creating a service ---

$svc = Service::create($conn, new ServiceConfig(
    name: 'calculator',
    version: '1.0.0',
    description: 'Calculator microservice',
    metadata: ['author' => 'team-php'],
    queueGroup: 'q',  // Load balancing
    errorHandler: function ($service, \Throwable $e) {
        echo "[SVC ERROR] {$e->getMessage()}\n";
    },
    doneHandler: function ($service) {
        echo "[SVC] Service stopped.\n";
    },
));

// --- Adding endpoints (Closure) ---

$svc->addEndpoint('add', function (RequestInterface $req) {
    $data = json_decode($req->data(), true);
    $result = ($data['a'] ?? 0) + ($data['b'] ?? 0);
    $req->respondJson(['result' => $result]);
}, new EndpointConfig(
    subject: 'calc.add',
    metadata: ['description' => 'Adds two numbers'],
));

$svc->addEndpoint('multiply', function (RequestInterface $req) {
    $data = json_decode($req->data(), true);
    $result = ($data['a'] ?? 0) * ($data['b'] ?? 0);
    $req->respondJson(['result' => $result]);
}, new EndpointConfig(subject: 'calc.multiply'));

// --- Handler class (implements HandlerInterface) ---

class DivideHandler implements HandlerInterface
{
    public function handle(RequestInterface $request): void
    {
        $data = json_decode($request->data(), true);
        $b = $data['b'] ?? 0;

        if ($b === 0) {
            $request->error('400', 'Division by zero');
            return;
        }

        $result = ($data['a'] ?? 0) / $b;
        $request->respondJson(['result' => $result]);
    }
}

$svc->addEndpoint('divide', new DivideHandler(), new EndpointConfig(
    subject: 'calc.divide',
));

// --- Grouping endpoints ---

$mathGroup = $svc->addGroup('math');
$mathGroup->addEndpoint('sqrt', function (RequestInterface $req) {
    $data = json_decode($req->data(), true);
    $req->respondJson(['result' => sqrt($data['n'] ?? 0)]);
});
// Subject will be: math.sqrt

// Nested groups
$advancedGroup = $mathGroup->addGroup('advanced');
$advancedGroup->addEndpoint('factorial', function (RequestInterface $req) {
    $data = json_decode($req->data(), true);
    $n = $data['n'] ?? 0;
    $result = 1;
    for ($i = 2; $i <= $n; $i++) { $result *= $i; }
    $req->respondJson(['result' => $result]);
});
// Subject will be: math.advanced.factorial

// --- Using the service (client side) ---

// Request-reply to call an endpoint
$reply = $conn->request('calc.add', json_encode(['a' => 5, 'b' => 3]), 2.0);
echo "5 + 3 = " . json_decode($reply->data, true)['result'] . "\n";

$reply = $conn->request('calc.multiply', json_encode(['a' => 4, 'b' => 7]), 2.0);
echo "4 * 7 = " . json_decode($reply->data, true)['result'] . "\n";

// --- Service discovery ---

// Ping - check if service is alive
$pingReply = $conn->request('$SRV.PING', '', 2.0);
echo "Ping: {$pingReply->data}\n";

// Ping specific service
$pingReply = $conn->request('$SRV.PING.calculator', '', 2.0);
echo "Calculator ping: {$pingReply->data}\n";

// Info - service details
$infoReply = $conn->request('$SRV.INFO.calculator', '', 2.0);
echo "Info: {$infoReply->data}\n";

// Stats - statistics
$statsReply = $conn->request('$SRV.STATS.calculator', '', 2.0);
echo "Stats: {$statsReply->data}\n";

// --- Programmatic access to info/stats ---

$info = $svc->info();
echo "Service: {$info->identity->name} v{$info->identity->version}\n";
echo "ID: {$info->identity->id}\n";
echo "Endpoints: " . count($info->endpoints) . "\n";

$stats = $svc->stats();
foreach ($stats->endpoints as $ep) {
    echo "  {$ep->name}: {$ep->numRequests} requests, {$ep->numErrors} errors\n";
}

// --- Reset statistics ---

$svc->reset();

// --- Graceful stop ---

$svc->stop();
echo "Stopped: " . ($svc->isStopped() ? 'yes' : 'no') . "\n";

$conn->close();
