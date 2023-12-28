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

use Amp\DeferredFuture;
use PHPinnacle\Ridge\Channel;
use PHPinnacle\Ridge\Client;
use PHPinnacle\Ridge\Exception;
use PHPinnacle\Ridge\Message;
use PHPinnacle\Ridge\Queue;
use Revolt\EventLoop;

class ChannelTest extends AsyncTest
{
    public function testOpenNotReadyChannel(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $future = $channel->open();

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testClose(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $future = $channel->close();

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testCloseAlreadyClosedChannel(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = $client->channel()->await();

        try {
            $channel->close()->await();
            $channel->close()->await();
        } finally {
            $client->disconnect()->await();
        }
    }

    public function testExchangeDeclare(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $future = $channel->exchangeDeclare('test_exchange', 'direct', false, false, true);

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testExchangeDelete(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->exchangeDeclare('test_exchange_no_ad', 'direct')->await();

        $future = $channel->exchangeDelete('test_exchange_no_ad');

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testQueueDeclare(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $future = $channel->queueDeclare('test_queue', false, false, false, true);

        self::assertFuture($future);

        /** @var Queue $queue */
        $queue = $future->await();

        self::assertInstanceOf(Queue::class, $queue);
        self::assertSame('test_queue', $queue->name());
        self::assertSame(0, $queue->messages());
        self::assertSame(0, $queue->consumers());

        $client->disconnect()->await();
    }

    public function testQueueBind(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->exchangeDeclare('test_exchange', 'direct', false, false, true)->await();
        $channel->queueDeclare('test_queue', false, false, false, true)->await();

        $future = $channel->queueBind('test_queue', 'test_exchange');

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testQueueUnbind(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->exchangeDeclare('test_exchange', 'direct', false, false, true)->await();
        $channel->queueDeclare('test_queue', false, false, false, true)->await();
        $channel->queueBind('test_queue', 'test_exchange')->await();

        $future = $channel->queueUnbind('test_queue', 'test_exchange');

        self::assertFuture($future);

        $future->await();

        $client->disconnect()->await();
    }

    public function testQueuePurge(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->queueDeclare('test_queue', false, false, false, true)->await();
        $channel->publish('test', '', 'test_queue')->await();
        $channel->publish('test', '', 'test_queue')->await();

        $future = $channel->queuePurge('test_queue');

        $messages = $future->await();

        self::assertFuture($future);
        self::assertEquals(2, $messages);

        $client->disconnect()->await();
    }

    public function testQueueDelete(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->queueDeclare('test_queue_no_ad')->await();
        $channel->publish('test', '', 'test_queue_no_ad')->await();

        $future = $channel->queueDelete('test_queue_no_ad');

        $messages = $future->await();

        self::assertFuture($future);
        self::assertEquals(1, $messages);

        $client->disconnect()->await();
    }

    public function testPublish(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $future = $channel->publish('test publish');

        self::assertFuture($future);
        self::assertNull($future->await());

        $client->disconnect()->await();
    }

    public function testMandatoryPublish(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $deferred = new DeferredFuture();
        $watcher  = EventLoop::delay(100, function () use ($deferred) {
            $deferred->complete(false);
        });

        $channel->events()->onReturn(function (Message $message) use ($deferred, $watcher) {
            self::assertSame($message->content, '.');
            self::assertSame($message->exchange, '');
            self::assertSame($message->routingKey, '404');
            self::assertSame($message->headers, []);
            self::assertNull($message->consumerTag);
            self::assertNull($message->deliveryTag);
            self::assertFalse($message->redelivered);
            self::assertTrue($message->returned);

            EventLoop::cancel($watcher);

            $deferred->complete(true);
        });

        $channel->publish('.', '', '404', [], true)->await();

        self::assertTrue($deferred->getFuture()->await(), 'Mandatory return event not received!');

        $client->disconnect()->await();
    }

    public function testImmediatePublish(Client $client)
    {
        $properties = $client->properties();

        // RabbitMQ 3 doesn't support "immediate" publish flag.
        if ($properties->product() === 'RabbitMQ' && version_compare($properties->version(), '3.0', '>')) {
            $client->disconnect()->await();

            return;
        }

        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $deferred = new DeferredFuture();
        $watcher  = EventLoop::delay(100, function () use ($deferred) {
            $deferred->complete(false);
        });

        $channel->events()->onReturn(function (Message $message) use ($deferred, $watcher) {
            self::assertTrue($message->returned);

            EventLoop::cancel($watcher);

            $deferred->complete(true);
        });

        $channel->queueDeclare('test_queue', false, false, false, true)->await();
        $channel->publish('.', '', 'test_queue', [], false, true)->await();

        self::assertTrue($deferred->getFuture()->await(), 'Immediate return event not received!');

        $client->disconnect()->await();
    }

    public function testConsume(Client $client)
    {
        /** @var Channel $channel */
        $channel = $client->channel()->await();

        $channel->queueDeclare('test_queue', false, false, false, true)->await();
        $channel->publish('hi', '', 'test_queue')->await();
    
        /** @noinspection PhpUnusedLocalVariableInspection */
        $tag = $channel->consume(function (Message $message) use ($client, &$tag) {
            self::assertEquals('hi', $message->content);
            self::assertEquals($tag, $message->consumerTag);

            $client->disconnect()->await();
        }, 'test_queue', false, true)->await();
    }

    public function testCancel(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('hi', '', 'test_queue');

        $tag = yield $channel->consume(function (Message $message) {
        }, 'test_queue', false, true);

        $promise = $channel->cancel($tag);

        self::assertPromise($promise);

        yield $promise;

        yield $client->disconnect();
    }

    public function testHeaders(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('<b>hi html</b>', '', 'test_queue', [
            'content-type' => 'text/html',
            'custom' => 'value',
        ]);

        yield $channel->consume(function (Message $message) use ($client) {
            self::assertEquals('text/html', $message->header('content-type'));
            self::assertEquals('value', $message->header('custom'));
            self::assertEquals('<b>hi html</b>', $message->content);

            yield $client->disconnect();
        }, 'test_queue', false, true);
    }

    public function testGet(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('get_test', false, false, false, true);

        yield $channel->publish('.', '', 'get_test');

        /** @var Message $message1 */
        $message1 = yield $channel->get('get_test', true);

        self::assertNotNull($message1);
        self::assertInstanceOf(Message::class, $message1);
        self::assertEquals('', $message1->exchange);
        self::assertEquals('.', $message1->content);
        self::assertEquals('get_test', $message1->routingKey);
        self::assertEquals(1, $message1->deliveryTag);
        self::assertNull($message1->consumerTag);
        self::assertFalse($message1->redelivered);
        self::assertIsArray($message1->headers);

        self::assertNull(yield $channel->get('get_test', true));

        yield $channel->publish('..', '', 'get_test');

        /** @var Message $message2 */
        $message2 = yield $channel->get('get_test');

        self::assertNotNull($message2);
        self::assertEquals(2, $message2->deliveryTag);
        self::assertFalse($message2->redelivered);

        $client->disconnect()->onResolve(function () use ($client) {
            yield $client->connect();

            /** @var Channel $channel */
            $channel = yield $client->channel();

            /** @var Message $message3 */
            $message3 = yield $channel->get('get_test');

            self::assertNotNull($message3);
            self::assertInstanceOf(Message::class, $message3);
            self::assertEquals('', $message3->exchange);
            self::assertEquals('..', $message3->content);
            self::assertTrue($message3->redelivered);

            yield $channel->ack($message3);

            yield $client->disconnect();
        });
    }

    public function testAck(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = yield $channel->get('test_queue');
        $promise = $channel->ack($message);

        self::assertPromise($promise);

        yield $promise;

        yield $client->disconnect();
    }

    public function testNack(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertFalse($message->redelivered);

        $promise = $channel->nack($message);

        self::assertPromise($promise);

        yield $promise;

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertTrue($message->redelivered);

        yield $channel->nack($message, false, false);

        self::assertNull(yield $channel->get('test_queue'));

        yield $client->disconnect();
    }

    public function testReject(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertFalse($message->redelivered);

        $promise = $channel->reject($message);

        self::assertPromise($promise);

        yield $promise;

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertTrue($message->redelivered);

        yield $channel->reject($message, false);

        self::assertNull(yield $channel->get('test_queue'));

        yield $client->disconnect();
    }

    public function testRecover(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);
        yield $channel->publish('.', '', 'test_queue');

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertFalse($message->redelivered);

        $promise = $channel->recover(true);

        self::assertPromise($promise);

        yield $promise;

        /** @var Message $message */
        $message = yield $channel->get('test_queue');

        self::assertNotNull($message);
        self::assertTrue($message->redelivered);

        yield $channel->ack($message);

        yield $client->disconnect();
    }

    public function testBigMessage(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('test_queue', false, false, false, true);

        $body = \str_repeat('a', 10 << 20); // 10 MiB

        yield $channel->publish($body, '', 'test_queue');

        yield $channel->consume(function (Message $message, Channel $channel) use ($body, $client) {
            self::assertEquals(\strlen($body), \strlen($message->content));

            yield $channel->ack($message);
            yield $client->disconnect();
        }, 'test_queue');
    }

    public function testGetDouble(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('get_test_double', false, false, false, true);
        yield $channel->publish('.', '', 'get_test_double');

        try {
            yield [
                $channel->get('get_test_double'),
                $channel->get('get_test_double'),
            ];
        } finally {
            yield $channel->queueDelete('get_test_double');

            yield $client->disconnect();
        }
    }

    public function testEmptyMessage(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('empty_body_message_test', false, false, false, true);
        yield $channel->publish('', '', 'empty_body_message_test');

        /** @var Message $message */
        $message = yield $channel->get('empty_body_message_test', true);

        self::assertNotNull($message);
        self::assertEquals('', $message->content);

        $count = 0;

        yield $channel->consume(function (Message $message, Channel $channel) use ($client, &$count) {
            self::assertEmpty($message->content);

            yield $channel->ack($message);

            if (++$count === 2) {
                yield $client->disconnect();
            }
        }, 'empty_body_message_test');

        yield $channel->publish('', '', 'empty_body_message_test');
        yield $channel->publish('', '', 'empty_body_message_test');
    }

    public function testTxs(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();

        yield $channel->queueDeclare('tx_test', false, false, false, true);

        yield $channel->txSelect();
        yield $channel->publish('.', '', 'tx_test');
        yield $channel->txCommit();

        /** @var Message $message */
        $message = yield $channel->get('tx_test', true);

        self::assertNotNull($message);
        self::assertInstanceOf(Message::class, $message);
        self::assertEquals('.', $message->content);

        $channel->publish('..', '', 'tx_test');
        $channel->txRollback();

        $nothing = yield $channel->get('tx_test', true);

        self::assertNull($nothing);

        yield $client->disconnect();
    }

    public function testTxSelectCannotBeCalledMultipleTimes(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = yield $client->channel();

        try {
            yield $channel->txSelect();
            yield $channel->txSelect();
        } finally {
            yield $client->disconnect();
        }
    }

    public function testTxCommitCannotBeCalledUnderNotTransactionMode(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = yield $client->channel();

        try {
            yield $channel->txCommit();
        } finally {
            yield $client->disconnect();
        }
    }

    public function testTxRollbackCannotBeCalledUnderNotTransactionMode(Client $client)
    {
        self::expectException(Exception\ChannelException::class);

        /** @var Channel $channel */
        $channel = yield $client->channel();

        try {
            yield $channel->txRollback();
        } finally {
            yield $client->disconnect();
        }
    }

    public function testConfirmMode(Client $client)
    {
        /** @var Channel $channel */
        $channel = yield $client->channel();
        $channel->events()->onAck(function (int $deliveryTag, bool $multiple) {
            self::assertEquals($deliveryTag, 1);
            self::assertFalse($multiple);
        });

        yield $channel->confirmSelect();

        $deliveryTag = yield $channel->publish('.');

        self::assertEquals($deliveryTag, 1);

        yield $client->disconnect();
    }
}
