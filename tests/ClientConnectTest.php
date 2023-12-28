<?php
/**
 * This file is part of PHPinnacle/Ridge.
 *
 * (c) PHPinnacle Team <dev@phpinnacle.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace PHPinnacle\Ridge\Tests;

use Amp\Socket\ConnectException;
use PHPinnacle\Ridge\Client;

class ClientConnectTest extends RidgeTest
{
    public function testConnect()
    {
        $client = self::client();

        $client->connect();

        self::assertTrue($client->isConnected());

        $client->disconnect();
    }

    public function testConnectFailure()
    {
        self::expectException(ConnectException::class);

        $client = Client::create('amqp://127.0.0.2:5673?timeout=1');

        $client->connect();
    }
}
