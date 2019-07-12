
namespace RedisSample.Applibs
{
    using Autofac;
    using RedisSample.App_Start;
    using RedisSample.Interface;
    using RedisSample.Model;
    using System;

    public class PubSubDispatcher<TEventStream> : IPubSubDispatcher<TEventStream>
                    where TEventStream : EventStream
    {
        public bool DispatchMessage(TEventStream stream)
        {
            try
            {
                using (var scope = AutofacConfig.Container.BeginLifetimeScope())
                {
                    var handler = scope.ResolveNamed<IPubSubHandler<TEventStream>>(stream.Type);
                    handler?.Handle(stream);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DispatchMessage Exception:{ex.Message}");
            }

            return false;
        }
    }
}
