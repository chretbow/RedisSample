namespace RedisSample.Applibs
{
    using Newtonsoft.Json;
    using RedisSample.Interface;
    using RedisSample.Model;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    internal class RedisConsumer
    {
        private IPubSubDispatcher<RedisEventStream> dispatcher;

        private IEnumerable<string> channelNames;

        public RedisConsumer(IEnumerable<string> channelNames, IPubSubDispatcher<RedisEventStream> dispatcher)
        {
            this.dispatcher = dispatcher;
            this.channelNames = channelNames;
        }

        public void Start()
        {
            this.channelNames.ToList().ForEach(channelName =>
            {
                NoSqlService.RedisConnections.GetSubscriber().Subscribe(channelName, (channel, message) =>
                {
                    var @event = JsonConvert.DeserializeObject<RedisEventStream>(Encoding.UTF8.GetString(message));
                    if (this.dispatcher.DispatchMessage(@event))
                    {
                        Console.WriteLine($"DispatchMessage Success:{message}");
                    }
                });
            });
        }
    }
}
