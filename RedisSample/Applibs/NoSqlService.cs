using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;

namespace RedisSample.Applibs
{
    public class NoSqlService
    {
        private static Lazy<ConnectionMultiplexer> lazyRedisConnections;

        public static ConnectionMultiplexer RedisConnections
        {
            get
            {
                if (lazyRedisConnections == null)
                {
                    NoSqlInit();
                }

                return lazyRedisConnections.Value;
            }
        }

        private static Lazy<RedLockFactory> lazyDistributedLockService;

        public static RedLockFactory DistributedLockService
        {
            get
            {
                if (lazyDistributedLockService == null)
                {
                    NoSqlInit();
                }

                return lazyDistributedLockService.Value;
            }
        }

        private static void NoSqlInit()
        {
            lazyRedisConnections = new Lazy<ConnectionMultiplexer>(() =>
            {
                var options = ConfigurationOptions.Parse($"{ConfigHelper.RedisConnectionString}");

                var muxer = ConnectionMultiplexer.Connect(options);
                muxer.ConnectionFailed += (sender, e) =>
                {
                    Console.WriteLine("redis failed: " + EndPointCollection.ToString(e.EndPoint) + "/" + e.ConnectionType);
                };
                muxer.ConnectionRestored += (sender, e) =>
                {
                    Console.WriteLine("redis restored: " + EndPointCollection.ToString(e.EndPoint) + "/" + e.ConnectionType);
                };

                return muxer;
            });

            lazyDistributedLockService = new Lazy<RedLockFactory>(() =>
            {
                return RedLockFactory.Create(new List<RedLockMultiplexer>() { lazyRedisConnections.Value });
            });
        }
    }
}
