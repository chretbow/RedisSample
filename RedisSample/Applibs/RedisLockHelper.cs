
namespace RedisSample.Applibs
{
    using RedLockNet;
    using System;

    public static class RedisLockHelper
    {
        private static string RedisAffixKey = "RedisSample";

        public static IRedLock GrabLock(int lockIndex, TimeSpan ttl, TimeSpan waitTime, TimeSpan retryTime)
        {
            string key = $"{RedisAffixKey}RedisLockHelper:{lockIndex}";
            return NoSqlService.DistributedLockService.CreateLock(key, ttl, waitTime, retryTime);
        }

        public static IRedLock GrabLock(int lockIndex)
        {
            return GrabLock(lockIndex, TimeSpan.FromSeconds(100), TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(300));
        }
    }
}
