namespace RedisSample.Applibs
{
    class ConfigHelper
    {
        public static string RedisDatabase = "15";
        public static string RedisConnectionString = "127.0.0.1:6379";

        public static string RedisPubChannel = "RedisSample1";
        public static string RedisSubChannels = "RedisSample1,RedisSample2,RedisSample3";
    }
}
