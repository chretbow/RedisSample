namespace RedisSample.Interface
{
    using RedisSample.Model;

    public interface IPubSubHandler<TEventStream>
        where TEventStream : EventStream
    {
        void Handle(TEventStream stream);
    }
}
