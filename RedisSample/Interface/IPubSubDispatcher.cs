namespace RedisSample.Interface
{
    using RedisSample.Model;

    public interface IPubSubDispatcher<TEventStream> 
        where TEventStream : EventStream
    {
        bool DispatchMessage(TEventStream stream);
    }
}
