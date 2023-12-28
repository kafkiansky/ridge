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

use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Message;
use Revolt\EventLoop;

class ClientTest extends AsyncTest
{
    public function testOpenChannel(Client $client)
    {
        self::assertFuture($future = $client->channel());
        self::assertInstanceOf(Channel::class, $future->await());

        $client->disconnect()->await();
    }

    public function testOpenMultipleChannel(Client $client)
    {
        /** @var Channel $channel1 */
        /** @var Channel $channel2 */
        $channel1 = $client->channel()->await();
        $channel2 = $client->channel()->await();

        self::assertInstanceOf(Channel::class, $channel1);
        self::assertInstanceOf(Channel::class, $channel2);
        self::assertNotEquals($channel1->id(), $channel2->id());

        /** @var Channel $channel3 */
        $channel3 = $client->channel()->await();

        self::assertInstanceOf(Channel::class, $channel3);
        self::assertNotEquals($channel1->id(), $channel3->id());
        self::assertNotEquals($channel2->id(), $channel3->id());

        $client->disconnect()->await();
    }

    public function testDisconnectWithBufferedMessages(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();
        $count   = 0;

        $channel->qos(0, 1000)->await();
        $channel->queueDeclare('disconnect_test', false, false, false, true)->await();
        $channel->consume(function (Message $message, Channel $channel) use ($client, &$count) {
            $channel->ack($message)->await();

            self::assertEquals(1, ++$count);

            $client->disconnect()->await();
        }, 'disconnect_test');

        $channel->publish('.', '', 'disconnect_test')->await();
        $channel->publish('.', '', 'disconnect_test')->await();
        $channel->publish('.', '', 'disconnect_test')->await();
    }
}
