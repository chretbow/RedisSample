
namespace RedisSample
{
    using RedisSample.Applibs;
    using RedisSample.Model;
    using System;
    using System.Windows.Forms;
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void btnSend_Click(object sender, System.EventArgs e)
        {
            RedisProducer.Publish("RedisSample1", new PressureTestContentEvent { Content = "RedisSample1", CreateDateTime = DateTime.Now});

            RedisProducer.Publish("RedisSample2", new PressureTestContentEvent { Content = "RedisSample2", CreateDateTime = DateTime.Now });

            RedisProducer.Publish("RedisSample3", new PressureTestContentEvent { Content = "RedisSample3", CreateDateTime = DateTime.Now });
        }
    }
}
