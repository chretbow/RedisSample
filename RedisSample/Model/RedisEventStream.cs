
namespace RedisSample.Model
{
    using RedisSample.Interface;

    public class RedisEventStream : EventStream
    {
        public RedisEventStream(string type, string data, long utcTimeStamp)
        {
            Type = type;
            Data = data;
            UtcTimeStamp = utcTimeStamp;
        }
    }

    public interface IRedisPubSubHandler : IPubSubHandler<RedisEventStream>
    {

    }
}
