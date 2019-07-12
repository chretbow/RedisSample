namespace RedisSample.Model
{
    public class EventStream
    {
        public string Type { get; set; }

        public string Data { get; set; }

        public long UtcTimeStamp { get; set; }
    }
}
