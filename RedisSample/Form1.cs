using Autofac;
using RedisSample.Domain.Model;
using RedisSample.Domain.Repository;
using System.Windows.Forms;

namespace RedisSample
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();

            using (var scope = App_Start.AutofacConfig.Container.BeginLifetimeScope())
            {
                var memberInfoRepo = scope.Resolve<IMemberPointRepository>();
                var memberPoint = new MemberPoint
                {
                    MemberId = 100001,
                    Point = 100000
                };
            }
        }
    }
}
