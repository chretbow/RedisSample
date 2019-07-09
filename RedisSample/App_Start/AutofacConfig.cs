using Autofac;
using RedisSample.Applibs;
using RedisSample.Domain.Repository;
using RedisSample.Persistent;
using System.Linq;
using System.Reflection;

namespace RedisSample.App_Start
{
    internal static class AutofacConfig
    {
        public static IContainer Container;

        public static void RegisterContainer()
        {
            var builder = new ContainerBuilder();

            builder.RegisterType<RedisMemberPointRepository>()
                .WithParameter("conn", NoSqlService.RedisConnections)
                .WithParameter("database", ConfigHelper.RedisDatabase)
                .As<IMemberPointRepository>()
                .SingleInstance();

            Container = builder.Build();
        }
    }
}
