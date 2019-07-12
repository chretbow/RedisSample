using Autofac;
using RedisSample.Applibs;
using RedisSample.Domain.Repository;
using RedisSample.Interface;
using RedisSample.Model;
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

            var asm = Assembly.GetExecutingAssembly();

            builder.RegisterType<RedisMemberPointRepository>()
                .WithParameter("conn", NoSqlService.RedisConnections)
                .WithParameter("database", ConfigHelper.RedisDatabase)
                .As<IMemberPointRepository>()
                .SingleInstance();

            builder.RegisterAssemblyTypes(asm)
                .Where(t => t.IsAssignableTo<IRedisPubSubHandler>())
                .Named<IPubSubHandler<RedisEventStream>>(t => t.Name.Replace("Handler", string.Empty))
                .SingleInstance();

            Container = builder.Build();
        }
    }
}
