<?php

namespace BeyondCode\LaravelWebSockets\WebSockets\Channels\ChannelManagers;

use BeyondCode\LaravelWebSockets\WebSockets\Channels\Channel;
use BeyondCode\LaravelWebSockets\WebSockets\Channels\ChannelManager;
use BeyondCode\LaravelWebSockets\WebSockets\Channels\PresenceChannel;
use BeyondCode\LaravelWebSockets\WebSockets\Channels\PrivateChannel;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;

class RedisChannelManager implements ChannelManager
{
    protected $redis;

    public function __construct()
    {
        $this->initConnection();
    }

    protected function initConnection()
    {
        $connection = config('websockets.redis_connection', null);

        if ($connection !== null && is_string($connection) && array_key_exists($connection, $connections = Redis::connections() ?? [])) {
            return $connections[$connection];
        }

        $this->redis = tap(Redis::connection(), function ($connection) {
            if ($connection === null)
                throw new \RuntimeException("Redis connection not found");
        });
    }

    protected function getAppIndexKey($appId)
    {
        return 'websockets:app:' . $appId;
    }

    protected function determineChannelClass(string $channelName): string
    {
        if (Str::startsWith($channelName, 'private-')) {
            return PrivateChannel::class;
        }

        if (Str::startsWith($channelName, 'presence-')) {
            return PresenceChannel::class;
        }

        return Channel::class;
    }


    public function find(string $appId, string $channelName): ?Channel
    {
        return ($channel = $this->redis->hget($this->getAppIndexKey($appId), $channelName)) !== null ? unserialize($channel) : null;
    }

    public function findOrCreate(string $appId, string $channelName): Channel
    {
        if (($channel = $this->find($appId, $channelName)) === null) {
            $channelClass = $this->determineChannelClass($channelName);

            $this->redis->hset($this->getAppIndexKey($appId), $channelName, serialize($channel = new $channelClass($channelName)));
        }

        return $channel;
    }

    public function getChannels(string $appId): array
    {
        return collect($this->redis->hgetall($appId))
            ->map(function ($channel) {
                return unserialize($channel);
            })->toArray();
    }

    public function getConnectionCount(string $appId): int
    {
        return collect($this->getChannels($appId))
            ->flatMap(function (Channel $channel) {
                return collect($channel->getSubscribedConnections())->pluck('socketId');
            })
            ->unique()
            ->count();
    }

    public function removeFromAllChannels(\Ratchet\ConnectionInterface $connection)
    {
        if (!isset($connection->app)) {
            return;
        }

        /*
         * Remove the connection from all channels.
         */
        collect($this->getChannels($connection->app->id))
            ->each(function (Channel $channel) use ($connection) {
                $channel->unsubscribe($connection);
            });

        /*
         * Delete channels that do not have any connections.
         */
        $channels = collect($this->getChannels($connection->app->id))
            ->reject(function (Channel $channel) {
                $channel->hasConnections();
            })
            ->values()
            ->toArray();

        if (count($channels) > 0)
            $this->redis->hdel($this->getAppIndexKey($connection->app->id), $channels);
    }
}
