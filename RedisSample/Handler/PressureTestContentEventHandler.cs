namespace RedisSample.Handler
{
    using Newtonsoft.Json;
    using RedisSample.Model;
    using System;
    using System.Windows.Forms;

    public class PressureTestContentEventHandler : IRedisPubSubHandler
    {
        public void Handle(RedisEventStream stream)
        {
            try
            {
                var @event = JsonConvert.DeserializeObject<PressureTestContentEvent>(stream.Data);
                MessageBox.Show($"PressureTestContentEvent: {@event.Content} SendDateTime: {@event.CreateDateTime.ToString("yyyy:MM:dd HH:mm:ss.fff")} ReceiveDateTime: {DateTime.Now.ToString("yyyy:MM:dd HH:mm:ss.fff")}");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"PressureTestContentEventHandler Exception:{ex.Message}");
            }
        }
    }
}
