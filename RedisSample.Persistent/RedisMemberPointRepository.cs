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

        public Tuple<Exception, MemberPoint> SortedSetDecrement(MemberPoint memberPoint)
        {
            throw new NotImplementedException();
        }

        public Tuple<Exception, MemberPoint> SortedSetIncrement(MemberPoint memberPoint)
        {
            throw new NotImplementedException();
        }
    }
}
