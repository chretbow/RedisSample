using Newtonsoft.Json;
using RedisSample.Domain.Model;
using RedisSample.Domain.Repository;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RedisSample.Persistent
{
    public class RedisMemberPointRepository : IMemberPointRepository
    {
        private string affixKey = "RedisSample";

        private ConnectionMultiplexer conn;

        private int database;

        public RedisMemberPointRepository(ConnectionMultiplexer conn, int database)
        {
            this.conn = conn;
            this.database = database;
        }

        public Tuple<Exception, bool> StringInsert(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringSet(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> StringInsert(MemberPoint memberPoint, TimeSpan expire)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringSet(GenerateKey(memberPoint.MemberId), memberPoint.Point, expire);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }
        public Tuple<Exception, bool> StringUpdate(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringSet(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> StringUpdate(MemberPoint memberPoint, TimeSpan expire)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringSet(GenerateKey(memberPoint.MemberId), memberPoint.Point, expire);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> StringIncrement(int memberId, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringIncrement(GenerateKey(memberId), changePoint);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> StringDecrement(int memberId, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.StringDecrement(GenerateKey(memberId), changePoint);
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }


        public Tuple<Exception, MemberPoint> StringFind(int member)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.StringGet(GenerateKey(member));
                    var memberPoint = new MemberPoint
                    {
                        MemberId = member,
                        Point = result.HasValue ? Convert.ToInt32(result) : -1
                    };

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> StringGet(int[] members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.StringGet(GenerateKeys(members));
                    var memberPoints = new List<MemberPoint>();
                    for(var i=0; i < members.Count(); i++)
                    {
                        if (result[i].HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = members[i],
                                Point = Convert.ToInt64(result[i])
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }
        public Tuple<Exception, IEnumerable<MemberPoint>> StringBatchGet(int[] members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    Dictionary<int, Task<RedisValue>> dic = new Dictionary<int, Task<RedisValue>>();
                    IBatch batch = redis.CreateBatch();
                    foreach (var member in members)
                    {
                        var task = batch.StringGetAsync(GenerateKey(member));
                        dic.Add(member, task);
                    }

                    batch.Execute();
                    batch.WaitAll(dic.Values.ToArray());
                    var memberPoints = dic.Where(x => x.Value.IsCompleted && x.Value.Result.HasValue).Select(x => new MemberPoint
                    {
                        MemberId = x.Key,
                        Point = Convert.ToInt64(x.Value.Result)
                    }).ToList();

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }
        public Tuple<Exception, bool> KeyDelete(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.KeyDelete(GenerateKey(memberId));
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        private T UseConnection<T>(Func<IDatabase, T> func)
        {
            var redis = conn.GetDatabase(this.database);
            return func(redis);
        }

        private string GenerateKey(int memberId)
        {
            return $"{affixKey}:MemberPoint:{memberId}";
        }

        private RedisKey[] GenerateKeys(IEnumerable<int> members)
        {
            return members.Select(x => (RedisKey)GenerateKey(x)).ToArray();
        }

        public Tuple<Exception, bool> HashInsert(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = new HashEntry[]
                    {
                        new HashEntry(nameof(memberPoint.MemberId), memberPoint.MemberId),
                        new HashEntry(nameof(memberPoint.Point), memberPoint.Point)
                    };
                    redis.HashSet(GenerateKey(memberPoint.MemberId), values);

                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> HashUpdate(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = new HashEntry[]
                    {
                        new HashEntry(nameof(memberPoint.MemberId), memberPoint.MemberId),
                        new HashEntry(nameof(memberPoint.Point), memberPoint.Point)
                    };
                    redis.HashSet(GenerateKey(memberPoint.MemberId), values);

                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> HashIncrement(int memberId, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.HashIncrement(GenerateKey(memberId), nameof(MemberPoint.Point), changePoint);

                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> HashDecrement(int memberId, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.HashDecrement(GenerateKey(memberId), nameof(MemberPoint.Point), changePoint);

                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, MemberPoint> HashFindAll(int member)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.HashGetAll(GenerateKey(member));
                    var memberPoint = new MemberPoint
                    {
                        MemberId = result.Where(x => x.Name == nameof(MemberPoint.MemberId)).Select(x => Convert.ToInt32(x.Value)).FirstOrDefault(),
                        Point = result.Where(x => x.Name == nameof(MemberPoint.Point)).Select(x => Convert.ToInt64(x.Value)).FirstOrDefault()
                    };

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, MemberPoint> HashFindField(int member)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.HashGet(GenerateKey(member), nameof(MemberPoint.Point));
                    MemberPoint memberPoint = null;
                    if (result.HasValue)
                    {
                        memberPoint = new MemberPoint
                        {
                            MemberId = member,
                            Point = Convert.ToInt64(result)
                        };
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> HashBatchGet(int[] members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    Dictionary<int, Task<HashEntry[]>> dic = new Dictionary<int, Task<HashEntry[]>>();
                    IBatch batch = redis.CreateBatch();
                    foreach (var member in members)
                    {
                        var task = batch.HashGetAllAsync(GenerateKey(member));
                        dic.Add(member, task);
                    }

                    batch.Execute();
                    batch.WaitAll(dic.Values.ToArray());
                    var memberPoints = dic.Where(x => x.Value.IsCompleted && x.Value.Result.FirstOrDefault(y => y.Name == nameof(MemberPoint.Point)).Value.HasValue).Select(x => new MemberPoint
                    {
                        MemberId = x.Key,
                        Point = Convert.ToInt64(x.Value.Result.Where(y => y.Name == nameof(MemberPoint.Point)).Select(y => Convert.ToInt64(y.Value)).FirstOrDefault())
                    }).ToList();

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, long> ListLeftPush(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListLeftPush(GenerateKey(memberPoint.MemberId), JsonConvert.SerializeObject(memberPoint));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> ListLeftPush(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var memberId = memberPoints.Select(x => x.MemberId).FirstOrDefault();
                    var values =  memberPoints.Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray();

                    var length = redis.ListLeftPush(GenerateKey(memberId), values);
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, string> ListFindByIndex(int memberId, long index)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.ListGetByIndex(GenerateKey(memberId), index);
                    return Tuple.Create<Exception, string>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, string>(ex, null);
            }
        }

        public Tuple<Exception, long> ListInsertAfter(MemberPoint indexMemberPoint, MemberPoint addMemberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListInsertAfter(GenerateKey(indexMemberPoint.MemberId), JsonConvert.SerializeObject(indexMemberPoint), JsonConvert.SerializeObject(addMemberPoint));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> ListInsertBefore(MemberPoint indexMemberPoint, MemberPoint addMemberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListInsertBefore(GenerateKey(indexMemberPoint.MemberId), JsonConvert.SerializeObject(indexMemberPoint), JsonConvert.SerializeObject(addMemberPoint));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> ListRange(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.ListRange(GenerateKey(memberId));
                    var memberPoints = new List<MemberPoint>();
                    foreach(var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(JsonConvert.DeserializeObject<MemberPoint>(value));
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, long> ListRemove(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListRemove(GenerateKey(memberPoint.MemberId), JsonConvert.SerializeObject(memberPoint));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, MemberPoint> ListLeftPop(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.ListLeftPop(GenerateKey(memberId));
                    MemberPoint memberPoint = null;
                    if (value.HasValue)
                    {
                        memberPoint = JsonConvert.DeserializeObject<MemberPoint>(value);
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, long> ListLength(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListLength(GenerateKey(memberId));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, MemberPoint> ListRightPopLeftPush(int sourceMember, int destinationMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.ListRightPopLeftPush(GenerateKey(sourceMember), GenerateKey(destinationMember));
                    return Tuple.Create<Exception, MemberPoint>(null, JsonConvert.DeserializeObject<MemberPoint>(value));
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, long> ListRightPush(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.ListRightPush(GenerateKey(memberPoint.MemberId), JsonConvert.SerializeObject(memberPoint));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> ListRightPush(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var memberId = memberPoints.Select(x => x.MemberId).FirstOrDefault();
                    var values = memberPoints.Select(x => (RedisValue)JsonConvert.SerializeObject(x)).ToArray();

                    var length = redis.ListRightPush(GenerateKey(memberId), values);
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, MemberPoint> ListRightPop(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.ListRightPop(GenerateKey(memberId));
                    MemberPoint memberPoint = null;
                    if (value.HasValue)
                    {
                        memberPoint = JsonConvert.DeserializeObject<MemberPoint>(value);
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, bool> KeyExpire(int memberId, TimeSpan expire)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.KeyExpire(GenerateKey(memberId), expire);
                    return Tuple.Create<Exception, bool>(null, result);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> SetAdd(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var result = redis.SetAdd(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, result);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, long> SetAdd(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var memberId = memberPoints.Select(x => x.MemberId).FirstOrDefault();
                    var values = memberPoints.Select(x => (RedisValue)x.Point).ToArray();

                    var length = redis.SetAdd(GenerateKey(memberId), values);
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineIntersect(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Intersect, GenerateKey(firstMember), GenerateKey(secondMember));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineUnion(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Union, GenerateKey(firstMember), GenerateKey(secondMember));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineDifference(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Difference, GenerateKey(firstMember), GenerateKey(secondMember));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }


        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineIntersect(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Intersect, GenerateKeys(members));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineUnion(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Union, GenerateKeys(members));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetCombineDifference(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetCombine(SetOperation.Difference, GenerateKeys(members));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, long> SetCombineIntersectAndStore(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Intersect, GenerateKey(firstMember), GenerateKey(firstMember), GenerateKey(secondMember));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SetCombineUnionAndStore(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Union, GenerateKey(firstMember), GenerateKey(firstMember), GenerateKey(secondMember));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SetCombineDifferenceAndStore(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Difference, GenerateKey(firstMember), GenerateKey(firstMember), GenerateKey(secondMember));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SetCombineIntersectAndStore(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Intersect, GenerateKey(members.FirstOrDefault()), GenerateKeys(members));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SetCombineUnionAndStore(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Union, GenerateKey(members.FirstOrDefault()), GenerateKeys(members));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SetCombineDifferenceAndStore(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetCombineAndStore(SetOperation.Difference, GenerateKey(members.FirstOrDefault()), GenerateKeys(members));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, bool> SetContains(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.SetContains(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetGetMembers(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetMembers(GenerateKey(memberId));
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = memberId,
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, bool> SetMove(int source, int destination, long point)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.SetMove(GenerateKey(source), GenerateKey(destination), point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, MemberPoint> SetPop(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetPop(GenerateKey(memberId));
                    MemberPoint memberPoint = null;
                    if (value.HasValue)
                    {
                        memberPoint = new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value)
                        };
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetPop(int memberId, int count)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetPop(GenerateKey(memberId), count);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = memberId,
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, MemberPoint> SetGetRandomMember(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetRandomMember(GenerateKey(memberId));
                    MemberPoint memberPoint = null;
                    if (value.HasValue)
                    {
                        memberPoint = new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value)
                        };
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPoint);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetGetRandomMember(int memberId, int count)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetRandomMembers(GenerateKey(memberId), count);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = memberId,
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, bool> SetRemove(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.SetRemove(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, long> SetRemove(IEnumerable<MemberPoint> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SetRemove(GenerateKey(members.Select(x => x.MemberId).FirstOrDefault()), members.Select(x => (RedisValue)x.Point).ToArray());
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetScan(MemberPoint memberPoint, int pageSize)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetScan(GenerateKey(memberPoint.MemberId), memberPoint.Point + "*", pageSize);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = memberPoint.MemberId,
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SetScan(MemberPoint memberPoint, int pageSize, int pageOffset)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SetScan(GenerateKey(memberPoint.MemberId), memberPoint.Point + "*", pageSize, pageOffset: pageOffset);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        if (value.HasValue)
                        {
                            memberPoints.Add(new MemberPoint
                            {
                                MemberId = memberPoint.MemberId,
                                Point = Convert.ToInt64(value)
                            });
                        }
                    }

                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, bool> SortedSetAdd(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.SortedSetAdd(GenerateKey(memberPoint.MemberId), nameof(memberPoint.Point), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, long> SortedSetAdd(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var sortedSetEntry = memberPoints.Select(x => new SortedSetEntry(nameof(MemberPoint.Point) + x.Point, x.Point)).ToArray();
                    var value = redis.SortedSetAdd(GenerateKey(memberPoints.Select(x => x.MemberId).FirstOrDefault()), sortedSetEntry);

                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetCombineAndStoreIntersect(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetCombineAndStore(SetOperation.Intersect, GenerateKey(secondMember), GenerateKey(firstMember), GenerateKey(secondMember), Aggregate.Sum);
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetCombineAndStoreUnion(int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetCombineAndStore(SetOperation.Union, GenerateKey(secondMember), GenerateKey(firstMember), GenerateKey(secondMember), Aggregate.Sum);
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetCombineAndStoreIntersect(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetCombineAndStore(SetOperation.Intersect, GenerateKey(members.FirstOrDefault()), GenerateKeys(members), aggregate: Aggregate.Sum);
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetCombineAndStoreUnion(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetCombineAndStore(SetOperation.Union, GenerateKey(members.FirstOrDefault()), GenerateKeys(members), aggregate: Aggregate.Sum);
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, MemberPoint> SortedSetDecrement(MemberPoint memberPoint, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetDecrement(GenerateKey(memberPoint.MemberId), nameof(MemberPoint.Point) + memberPoint.Point, changePoint);
                    MemberPoint memberPointResult = new MemberPoint
                    {
                        MemberId = memberPoint.MemberId,
                        Point = Convert.ToInt64(value)
                    };
                    return Tuple.Create<Exception, MemberPoint>(null, memberPointResult);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, MemberPoint> SortedSetIncrement(MemberPoint memberPoint, long changePoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetIncrement(GenerateKey(memberPoint.MemberId), nameof(MemberPoint.Point) + memberPoint.Point, changePoint);
                    MemberPoint memberPointResult = new MemberPoint
                    {
                        MemberId = memberPoint.MemberId,
                        Point = Convert.ToInt64(value)
                    };
                    return Tuple.Create<Exception, MemberPoint>(null, memberPointResult);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, long> SortedSetLength(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetLength(GenerateKey(memberId));
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetLengthByValue(int memberId, string start, string end)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var value = redis.SortedSetLengthByValue(GenerateKey(memberId), nameof(MemberPoint.Point) + start, nameof(MemberPoint.Point) + end);
                    return Tuple.Create<Exception, long>(null, value);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, MemberPoint> SortedSetPop(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var entry = redis.SortedSetPop(GenerateKey(memberId));

                    MemberPoint memberPointResult = null;
                    if (entry.HasValue)
                    {
                        memberPointResult = new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(entry.Value.Score)
                        };
                    }

                    return Tuple.Create<Exception, MemberPoint>(null, memberPointResult);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, MemberPoint>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SortedSetPop(int memberId, long count)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetPop(GenerateKey(memberId), count);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        memberPoints.Add(new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value.Score)
                        });
                    }
                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<string>> SortedSetRangeByRank(int memberId, long start, long stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByRank(GenerateKey(memberId), start, stop);
                    return Tuple.Create<Exception, IEnumerable<string>>(null, values.Select(x => x.ToString()).ToList());
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<string>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SortedSetRangeByRankWithScores(int memberId, long start, long stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByRankWithScores(GenerateKey(memberId), start, stop);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        memberPoints.Add(new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value.Score)
                        });
                    }
                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<string>> SortedSetRangeByScore(int memberId, double start, double stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByScore(GenerateKey(memberId), start, stop);
                    return Tuple.Create<Exception, IEnumerable<string>>(null, values.Select(x => x.ToString()).ToList());
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<string>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SortedSetRangeByScoreWithScores(int memberId, double start, double stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByScoreWithScores(GenerateKey(memberId), start, stop);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        memberPoints.Add(new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value.Score)
                        });
                    }
                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<string>> SortedSetRangeByValue(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByValue(GenerateKey(memberId));
                    return Tuple.Create<Exception, IEnumerable<string>>(null, values.Select(x => x.ToString()).ToList());
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<string>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<string>> SortedSetRangeByValue(int memberId, string start, string stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetRangeByValue(GenerateKey(memberId), nameof(MemberPoint.Point) + start, nameof(MemberPoint.Point) + stop);
                    return Tuple.Create<Exception, IEnumerable<string>>(null, values.Select(x => x.ToString()).ToList());
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<string>>(ex, null);
            }
        }

        public Tuple<Exception, long> SortedSetRank(int memberId, string value)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var rank = redis.SortedSetRank(GenerateKey(memberId), nameof(MemberPoint.Point) + value);
                    return Tuple.Create<Exception, long>(null, rank ?? -1);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetRemove(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var count = redis.SortedSetRemove(GenerateKey(memberPoints.Select(x => x.MemberId).FirstOrDefault()), memberPoints.Select(x => (RedisValue)(nameof(MemberPoint.Point) + x.Point)).ToArray());
                    return Tuple.Create<Exception, long>(null, count);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, bool> SortedSetRemove(MemberPoint memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.SortedSetRemove(GenerateKey(memberPoints.MemberId), nameof(MemberPoint.Point) + memberPoints.Point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, long> SortedSetRemoveRangeByRank(int memberId, long start, long stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var count = redis.SortedSetRemoveRangeByRank(GenerateKey(memberId), start, stop);
                    return Tuple.Create<Exception, long>(null, count);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetRemoveRangeByScore(int memberId, double start, double stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var count = redis.SortedSetRemoveRangeByScore(GenerateKey(memberId), start, stop);
                    return Tuple.Create<Exception, long>(null, count);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> SortedSetRemoveRangeByValue(int memberId, string start, string stop)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var count = redis.SortedSetRemoveRangeByValue(GenerateKey(memberId), nameof(MemberPoint.Point) + start, nameof(MemberPoint.Point) + stop);
                    return Tuple.Create<Exception, long>(null, count);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SortedSetScan(int memberId, string pattern, int pageSize)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetScan(GenerateKey(memberId), nameof(MemberPoint.Point) + pattern, pageSize);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        memberPoints.Add(new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value.Score)
                        });
                    }
                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, IEnumerable<MemberPoint>> SortedSetScan(int memberId, string pattern, int pageSize, int pageOffset)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = redis.SortedSetScan(GenerateKey(memberId), nameof(MemberPoint.Point) + pattern, pageSize, pageOffset: pageOffset);
                    var memberPoints = new List<MemberPoint>();
                    foreach (var value in values)
                    {
                        memberPoints.Add(new MemberPoint
                        {
                            MemberId = memberId,
                            Point = Convert.ToInt64(value.Score)
                        });
                    }
                    return Tuple.Create<Exception, IEnumerable<MemberPoint>>(null, memberPoints);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, IEnumerable<MemberPoint>>(ex, null);
            }
        }

        public Tuple<Exception, double> SortedSetScore(int memberId, string value)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var score = redis.SortedSetScore(GenerateKey(memberId), nameof(MemberPoint.Point) + value);
                    return Tuple.Create<Exception, double>(null, score ?? -1);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, double>(ex, -1);
            }
        }

        public Tuple<Exception, bool> HyperLogLogAdd(IEnumerable<MemberPoint> memberPoints)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.HyperLogLogAdd(GenerateKey(memberPoints.Select(x => x.MemberId).FirstOrDefault()), memberPoints.Select(x => (RedisValue)x.Point).ToArray());
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> HyperLogLogAdd(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var flag = redis.HyperLogLogAdd(GenerateKey(memberPoint.MemberId), memberPoint.Point);
                    return Tuple.Create<Exception, bool>(null, flag);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, long> HyperLogLogLength(int memberId)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.HyperLogLogLength(GenerateKey(memberId));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, long> HyperLogLogLength(IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var length = redis.HyperLogLogLength(GenerateKeys(members));
                    return Tuple.Create<Exception, long>(null, length);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, long>(ex, -1);
            }
        }

        public Tuple<Exception, bool> HyperLogLogMerge(int destinationMember, int firstMember, int secondMember)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.HyperLogLogMerge(GenerateKey(destinationMember), GenerateKey(firstMember), GenerateKey(secondMember));
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> HyperLogLogMerge(int destinationMember, IEnumerable<int> members)
        {
            try
            {
                return UseConnection(redis =>
                {
                    redis.HyperLogLogMerge(GenerateKey(destinationMember), GenerateKeys(members));
                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> TransactionStringSet(IEnumerable<MemberPoint> oldPoints, long newPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var tran = redis.CreateTransaction();

                    foreach(var member in oldPoints)
                    {
                        //判斷有值才改
                        tran.AddCondition(Condition.StringEqual(GenerateKey(member.MemberId), member.Point)); // 樂觀鎖 
                        tran.StringSetAsync(GenerateKey(member.MemberId), newPoint);
                    }

                    bool committed = tran.Execute();
                    return Tuple.Create<Exception, bool>(null, committed);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> LuaAddIfNotExist(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = new RedisValue[]
                    {
                        memberPoint.MemberId,
                        memberPoint.Point
                    };

                    string script =
                    @"
                    if tonumber(redis.call('EXISTS', KEYS[1])) == 1  then return 0 end
                    redis.call('HMSET', KEYS[1]
                    , 'MemberId', ARGV[1]
                    , 'Point', ARGV[2])
                    redis.call('EXPIRE', KEYS[1], 3600)
                    return 1";

                    //// redis.call('EXPIRE', KEYS[1], 3600) 3600 is TTL
                    var result = (bool)redis.ScriptEvaluate(script, GenerateKeys(new List<int> { memberPoint.MemberId }), values);

                    return Tuple.Create<Exception, bool>(null, result);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> LuaUpdateIfExist(MemberPoint memberPoint)
        {
            try
            {
                return UseConnection(redis =>
                {
                    var values = new RedisValue[]
                    {
                        memberPoint.MemberId,
                        memberPoint.Point
                    };

                    string script =
                    @"
                    if tonumber(redis.call('EXISTS', KEYS[1])) == 0  then return 0 end
                    redis.call('HINCRBY', KEYS[1]
                    , 'Point', ARGV[2])
                    return 1";

                    var result = (bool)redis.ScriptEvaluate(script, GenerateKeys(new List<int> { memberPoint.MemberId }), values);

                    return Tuple.Create<Exception, bool>(null, result);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }

        public Tuple<Exception, bool> LockStringGet(int member)
        {
            try
            {
                return UseConnection(redis =>
                {
                    //lock_key表示的是redis數據庫中該鎖的名稱，不可重複。
                    //token用來標識誰擁有該鎖並用來釋放鎖。
                    //TimeSpan表示該鎖的有效時間。10秒後自動釋放，避免死鎖。
                    if (redis.LockTake(nameof(MemberPoint.MemberId), GenerateKey(member), TimeSpan.FromSeconds(10)))
                    {
                        try
                        {
                            var result = redis.StringGet(GenerateKey(member));
                        }
                        finally
                        {
                            redis.LockRelease(nameof(MemberPoint.MemberId), GenerateKey(member)); // 釋放鎖
                        }
                    }

                    return Tuple.Create<Exception, bool>(null, true);
                });
            }
            catch (Exception ex)
            {
                return Tuple.Create<Exception, bool>(ex, false);
            }
        }
    }
}
