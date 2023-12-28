<?php
/**
 * This file is part of PHPinnacle/Ridge.
 *
 * (c) PHPinnacle Team <dev@phpinnacle.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace PHPinnacle\Ridge;

use Amp\DeferredFuture;
use Amp\Future;
use PHPinnacle\Ridge\Exception\ConnectionException;
use Revolt\EventLoop;
use function Amp\async;

final class Client
{
    private const STATE_NOT_CONNECTED = 0;
    private const STATE_CONNECTING = 1;
    private const STATE_CONNECTED = 2;
    private const STATE_DISCONNECTING = 3;

    private const CONNECTION_MONITOR_INTERVAL = 5000;

    /**
     * @var Config
     */
    private $config;

    /**
     * @var int
     */
    private $state = self::STATE_NOT_CONNECTED;

    /**
     * @var Channel[]
     */
    private $channels = [];

    /**
     * @var int
     */
    private $nextChannelId = 1;

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var Properties
     */
    private $properties;

    /**
     * @var string|null
     */
    private $connectionMonitorWatcherId;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public static function create(string $dsn): self
    {
        return new self(Config::parse($dsn));
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function properties(): Properties
    {
        if ($this->state !== self::STATE_CONNECTED) {
            throw Exception\ClientException::notConnected();
        }

        return $this->properties;
    }

    /**
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function connect(): void
    {
        if ($this->state !== self::STATE_NOT_CONNECTED) {
            throw Exception\ClientException::alreadyConnected();
        }

        $this->state = self::STATE_CONNECTING;

        $this->connection = new Connection($this->config->uri());

        $this->connection->open(
            $this->config->timeout,
            $this->config->tcpAttempts,
            $this->config->tcpNoDelay
        );

        $buffer = new Buffer;
        $buffer
            ->append('AMQP')
            ->appendUint8(0)
            ->appendUint8(0)
            ->appendUint8(9)
            ->appendUint8(1);

        $this->connection->write($buffer);

        $this->connectionStart()->await();
        $this->connectionTune()->await();
        $this->connectionOpen()->await();

        $this->future(Protocol\ConnectionCloseFrame::class)->map(function () {
            $buffer = new Buffer;
            $buffer
                ->appendUint8(1)
                ->appendUint16(0)
                ->appendUint32(4)
                ->appendUint16(10)
                ->appendUint16(51)
                ->appendUint8(206);

            $this->connection->write($buffer);
            $this->connection->close();

            $this->disableConnectionMonitor();
        });

        $this->state = self::STATE_CONNECTED;

        $this->connectionMonitorWatcherId =  EventLoop::repeat(
            self::CONNECTION_MONITOR_INTERVAL,
            function(): void
            {
                if($this->connection->connected() === false) {
                    $this->state = self::STATE_NOT_CONNECTED;

                    throw Exception\ClientException::disconnected();
                }
            }
        );
    }

    /**
     * @return Future<void>
     *
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function disconnect(int $code = 0, string $reason = ''): Future
    {
        $this->disableConnectionMonitor();

        return async(
            function () use ($code, $reason) {
                try {
                    if (\in_array($this->state, [self::STATE_NOT_CONNECTED, self::STATE_DISCONNECTING])) {
                        return;
                    }

                    if ($this->state !== self::STATE_CONNECTED) {
                        throw Exception\ClientException::notConnected();
                    }

                    if($this->connectionMonitorWatcherId !== null){
                        EventLoop::cancel($this->connectionMonitorWatcherId);

                        $this->connectionMonitorWatcherId = null;
                    }

                    $this->state = self::STATE_DISCONNECTING;

                    if ($code === 0) {
                        $futures = [];

                        foreach ($this->channels as $channel) {
                            $futures[] = $channel->close($code, $reason);
                        }

                        Future\awaitAll($futures);
                    }

                    $this->connectionClose($code, $reason)->await();

                    $this->connection->close();
                }
                finally
                {
                    $this->state = self::STATE_NOT_CONNECTED;
                }
            }
        );
    }

    /**
     * @return Future<Channel>
     *
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    public function channel(): Future
    {
        return async(
            function () {
                if ($this->state !== self::STATE_CONNECTED) {
                    throw Exception\ClientException::notConnected();
                }

                try {
                    $id = $this->findChannelId();
                    $channel = new Channel($id, $this->connection, $this->properties);

                    $this->channels[$id] = $channel;

                    $channel->open()->await();
                    $channel->qos($this->config->qosSize, $this->config->qosCount, $this->config->qosGlobal)->await();

                    EventLoop::queue(function () use ($id): void {
                        /** @var Protocol\ChannelCloseFrame|Protocol\ChannelCloseOkFrame $frame */
                        $frame = Future\awaitFirst([
                            $this->future(Protocol\ChannelCloseFrame::class, $id),
                            $this->future(Protocol\ChannelCloseOkFrame::class, $id)
                        ]);

                        $this->connection->cancel($id);

                        if ($frame instanceof Protocol\ChannelCloseFrame) {
                            $buffer = new Buffer;
                            $buffer
                                ->appendUint8(1)
                                ->appendUint16($id)
                                ->appendUint32(4)
                                ->appendUint16(20)
                                ->appendUint16(41)
                                ->appendUint8(206);

                            $this->connection->write($buffer);
                        }

                        unset($this->channels[$id]);
                    });

                    return $channel;
                }
                catch(ConnectionException $exception) {
                    $this->state = self::STATE_NOT_CONNECTED;

                    throw $exception;
                }
                catch (\Throwable $error) {
                    throw Exception\ClientException::unexpectedResponse($error);
                }
            }
        );
    }

    public function isConnected(): bool
    {
        return $this->state === self::STATE_CONNECTED && $this->connection->connected();
    }

    /**
     * @return Future<void>
     *
     * @throws \PHPinnacle\Ridge\Exception\ClientException
     */
    private function connectionStart(): Future
    {
        return async(
            function () {
                /** @var Protocol\ConnectionStartFrame $start */
                $start = $this->future(Protocol\ConnectionStartFrame::class)->await();

                if (!\str_contains($start->mechanisms, 'AMQPLAIN')) {
                    throw Exception\ClientException::notSupported($start->mechanisms);
                }

                $this->properties = Properties::create($start->serverProperties);

                $buffer = new Buffer;
                $buffer
                    ->appendTable([
                        'LOGIN' => $this->config->user,
                        'PASSWORD' => $this->config->pass,
                    ])
                    ->discard(4);

                $frameBuffer = new Buffer;
                $frameBuffer
                    ->appendUint16(10)
                    ->appendUint16(11)
                    ->appendTable([])
                    ->appendString('AMQPLAIN')
                    ->appendText((string)$buffer)
                    ->appendString('en_US');

                $this->connection->method(0, $frameBuffer);
            }
        );
    }

    /**
     * @return Future<void>
     */
    private function connectionTune(): Future
    {
        return async(
            function () {
                /** @var Protocol\ConnectionTuneFrame $tune */
                $tune = $this->future(Protocol\ConnectionTuneFrame::class)->await();

                $heartbeatInterval = $this->config->heartbeat;

                if ($heartbeatInterval !== 0) {
                    $heartbeatInterval = \min($heartbeatInterval, $tune->heartbeat * 1000);
                }

                $maxChannel = \min($this->config->maxChannel, $tune->channelMax);
                $maxFrame = \min($this->config->maxFrame, $tune->frameMax);

                $buffer = new Buffer;
                $buffer
                    ->appendUint8(1)
                    ->appendUint16(0)
                    ->appendUint32(12)
                    ->appendUint16(10)
                    ->appendUint16(31)
                    ->appendInt16($maxChannel)
                    ->appendInt32($maxFrame)
                    ->appendInt16((int) ($heartbeatInterval / 1000))
                    ->appendUint8(206);

                $this->connection->write($buffer);

                $this->properties->tune($maxChannel, $maxFrame);

                if ($heartbeatInterval > 0) {
                    $this->connection->heartbeat($heartbeatInterval);
                }
            }
        );
    }

    /**
     * @return Future<Protocol\ConnectionOpenOkFrame>
     */
    private function connectionOpen(): Future
    {
        return async(
            function () {
                $vhost = $this->config->vhost;
                $capabilities = '';
                $insist = false;

                $buffer = new Buffer;
                $buffer
                    ->appendUint8(1)
                    ->appendUint16(0)
                    ->appendUint32(7 + \strlen($vhost) + \strlen($capabilities))
                    ->appendUint16(10)
                    ->appendUint16(40)
                    ->appendString($vhost)
                    ->appendString($capabilities) // TODO: process server capabilities
                    ->appendBits([$insist])
                    ->appendUint8(206);

                $this->connection->write($buffer);

                return $this->future(Protocol\ConnectionOpenOkFrame::class)->await();
            }
        );
    }

    /**
     * @return Future<void>
     */
    private function connectionClose(int $code, string $reason): Future
    {
        return async(
            function () use ($code, $reason) {
                $buffer = new Buffer;
                $buffer
                    ->appendUint8(1)
                    ->appendUint16(0)
                    ->appendUint32(11 + \strlen($reason))
                    ->appendUint16(10)
                    ->appendUint16(50)
                    ->appendInt16($code)
                    ->appendString($reason)
                    ->appendInt16(0)
                    ->appendInt16(0)
                    ->appendUint8(206);

                $this->connection->write($buffer);

                $this->future(Protocol\ConnectionCloseOkFrame::class)->await();
            }
        );
    }

    /**
     * @return int
     */
    private function findChannelId(): int
    {
        /** first check in range [next, max] ... */
        for ($id = $this->nextChannelId; $id <= $this->config->maxChannel; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        /** then check in range [min, next) ... */
        for ($id = 1; $id < $this->nextChannelId; ++$id) {
            if (!isset($this->channels[$id])) {
                $this->nextChannelId = $id + 1;

                return $id;
            }
        }

        throw Exception\ClientException::noChannelsAvailable();
    }

    /**
     * @template T of Protocol\AbstractFrame
     * @psalm-param class-string<T> $frame
     * @psalm-return Future<T>
     */
    private function future(string $frame, int $channel = 0): Future
    {
        /** @psalm-var DeferredFuture<T> $deferred */
        $deferred = new DeferredFuture();

        $this->connection->subscribe(
            $channel,
            $frame,
            static function (Protocol\AbstractFrame $frame) use ($deferred) {
                /** @psalm-var T $frame */
                $deferred->complete($frame);

                return true;
            }
        );

        return $deferred->getFuture();
    }

    private function disableConnectionMonitor(): void {
        if($this->connectionMonitorWatcherId !== null) {

            EventLoop::cancel($this->connectionMonitorWatcherId);

            $this->connectionMonitorWatcherId = null;
        }
    }
}
