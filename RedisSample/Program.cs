using RedisSample.App_Start;
using RedisSample.Applibs;
using RedisSample.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace RedisSample
{
    static class Program
    {
        /// <summary>
        /// 應用程式的主要進入點。
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            AutofacConfig.RegisterContainer();

            var channels = ConfigHelper.RedisSubChannels.Split(',');

            var consumer = new RedisConsumer(
                channels,
                new PubSubDispatcher<RedisEventStream>());
            consumer.Start();

            Application.Run(new Form1());
        }
    }
}
