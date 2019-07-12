namespace RedisSample.Applibs
{
    using Newtonsoft.Json;
    using RedisSample.Model;
    using System;
    using System.Text;

    internal class RedisProducer
    {
        public static void Publish<T>(string channelName, T data)
        {
            var es = new RedisEventStream(
                typeof(T).Name,
                JsonConvert.SerializeObject(data),
                TimeStampHelper.ToUtcTimeStamp(DateTime.Now));

            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(es));
            NoSqlService.RedisConnections.GetSubscriber().Publish(channelName, body);
        }
    }
}
