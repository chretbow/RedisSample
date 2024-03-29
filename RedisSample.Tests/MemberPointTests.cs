﻿using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RedisSample.Domain.Model;
using RedisSample.Domain.Repository;
using RedisSample.Persistent;
using StackExchange.Redis;

namespace RedisSample.Tests
{
    [TestClass]
    public class MemberPointTests
    {
        private ConnectionMultiplexer redis;

        public MemberPointTests()
        {
            redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
        }

        [TestMethod]
        public void StringTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 0);

            var result = repo.StringInsert(new MemberPoint
            {
                MemberId = 100000,
                Point = 10000
            });

            Assert.IsNull(result.Item1);
            Assert.IsTrue(result.Item2);

            var ttlResult = repo.StringInsert(new MemberPoint
            {
                MemberId = 100002,
                Point = 20000
            }, TimeSpan.FromSeconds(30));

            Assert.IsNull(ttlResult.Item1);
            Assert.IsTrue(ttlResult.Item2);

            var updateResult = repo.StringUpdate(new MemberPoint
            {
                MemberId = 100000,
                Point = 11111
            });

            Assert.IsNull(updateResult.Item1);
            Assert.IsTrue(updateResult.Item2);

            var ttlUpdateResult = repo.StringUpdate(new MemberPoint
            {
                MemberId = 100002,
                Point = 22222
            }, TimeSpan.FromSeconds(30));

            Assert.IsNull(ttlUpdateResult.Item1);
            Assert.IsTrue(ttlUpdateResult.Item2);

            //有設定TTL的資料不適合作Increment or Decrement，因為原數據消失後，可能會產生錯誤的數據
            var incrementResult = repo.StringIncrement(100000, 100);
            Assert.IsNull(incrementResult.Item1);
            Assert.IsTrue(incrementResult.Item2);

            var decrementResult = repo.StringDecrement(100000, 1000);
            Assert.IsNull(decrementResult.Item1);
            Assert.IsTrue(decrementResult.Item2);

            var findResult = repo.StringFind(100000);
            Assert.IsNull(findResult.Item1);
            Assert.IsNotNull(findResult.Item2);

            var getResult = repo.StringGet(new int[] { 100000, 100002, 100003 });
            Assert.IsNull(getResult.Item1);
            Assert.IsNotNull(getResult.Item2);

            var batchGetResult = repo.StringBatchGet(new int[] { 100000, 100002, 100003 });
            Assert.IsNull(batchGetResult.Item1);
            Assert.IsNotNull(batchGetResult.Item2);

            var delResult = repo.KeyDelete(100000);
            Assert.IsNull(delResult.Item1);
            Assert.IsTrue(delResult.Item2);
        }

        [TestMethod]
        public void HashTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 1);

            var result = repo.HashInsert(new MemberPoint
            {
                MemberId = 100000,
                Point = 10000
            });

            Assert.IsNull(result.Item1);
            Assert.IsTrue(result.Item2);

            var updateResult = repo.HashUpdate(new MemberPoint
            {
                MemberId = 100000,
                Point = 11111
            });

            Assert.IsNull(updateResult.Item1);
            Assert.IsTrue(updateResult.Item2);

            var incrementResult = repo.HashIncrement(100000, 100);
            Assert.IsNull(incrementResult.Item1);
            Assert.IsTrue(incrementResult.Item2);

            var decrementResult = repo.HashDecrement(100000, 1000);
            Assert.IsNull(decrementResult.Item1);
            Assert.IsTrue(decrementResult.Item2);

            var findResult = repo.HashFindAll(100000);
            Assert.IsNull(findResult.Item1);
            Assert.IsNotNull(findResult.Item2);

            var findFieldResult = repo.HashFindField(100000);
            Assert.IsNull(findFieldResult.Item1);
            Assert.IsNotNull(findFieldResult.Item2);

            var batchGetResult = repo.HashBatchGet(new int[] { 100000, 100002, 100003 });
            Assert.IsNull(batchGetResult.Item1);
            Assert.IsNotNull(batchGetResult.Item2);

            var expireResult = repo.KeyExpire(100000, TimeSpan.FromSeconds(30));
            Assert.IsNull(expireResult.Item1);
            Assert.IsTrue(expireResult.Item2);

            //var delResult = repo.KeyDelete(100000);
            //Assert.IsNull(delResult.Item1);
            //Assert.IsTrue(delResult.Item2);
        }

        [TestMethod]
        public void ListTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 2);

            // 從Index小(最上面)的那邊加
            var result = repo.ListLeftPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 10000
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListLeftPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 10001
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListLeftPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 10002
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListLeftPush(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100001, Point = 10005 },
                new MemberPoint { MemberId = 100001, Point = 10006 },
                new MemberPoint { MemberId = 100001, Point = 10007 },
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            // 0 base
            var findByIndexResult = repo.ListFindByIndex(100000, 0);

            Assert.IsNull(findByIndexResult.Item1);
            Assert.IsNotNull(findByIndexResult.Item2);

            // 若value有多筆一樣，會以找到第一筆去塞在後面
            var insertAfterResult = repo.ListInsertAfter(
                new MemberPoint
                {
                    MemberId = 100000,
                    Point = 10002
                }, new MemberPoint
                {
                    MemberId = 100000,
                    Point = 10003
                });

            Assert.IsNull(insertAfterResult.Item1);
            Assert.AreNotEqual(insertAfterResult.Item2, -1);

            // 若value有多筆一樣，會以找到第一筆去塞在前面
            var insertBeforeResult = repo.ListInsertBefore(
                new MemberPoint
                {
                    MemberId = 100000,
                    Point = 10003
                }, new MemberPoint
                {
                    MemberId = 100000,
                    Point = 10004
                });

            Assert.IsNull(insertBeforeResult.Item1);
            Assert.AreNotEqual(insertBeforeResult.Item2, -1);

            var rangeResult = repo.ListRange(100000);

            Assert.IsNull(rangeResult.Item1);
            Assert.IsNotNull(rangeResult.Item2);

            // 所有相同內容都會被刪掉
            var removeResult = repo.ListRemove(new MemberPoint
            {
                MemberId = 100000,
                Point = 10002
            });

            Assert.IsNull(removeResult.Item1);
            Assert.AreNotEqual(removeResult.Item2, -1);

            var leftPopResult = repo.ListLeftPop(100000);
            Assert.IsNull(leftPopResult.Item1);
            Assert.IsNotNull(leftPopResult.Item2);

            var lengthResult = repo.ListLength(100000);

            Assert.IsNull(lengthResult.Item1);
            Assert.AreNotEqual(lengthResult.Item2, -1);

            // 將來源LIST的最後一筆，搬到目的LIST的第一筆
            var rightPopLeftPushResult = repo.ListRightPopLeftPush(100000, 100000);

            Assert.IsNull(rightPopLeftPushResult.Item1);
            Assert.IsNotNull(leftPopResult.Item2);

            // 從Index大(最下面)的那邊加
            result = repo.ListRightPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 11000
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListRightPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 12000
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListRightPush(new MemberPoint
            {
                MemberId = 100000,
                Point = 13000
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.ListRightPush(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100000, Point = 14000 },
                new MemberPoint { MemberId = 100000, Point = 15000 },
                new MemberPoint { MemberId = 100000, Point = 16000 }
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            var rightPopResult = repo.ListRightPop(100000);
            Assert.IsNull(rightPopResult.Item1);
            Assert.IsNotNull(rightPopResult.Item2);

            var expireResult = repo.KeyExpire(100002, TimeSpan.FromSeconds(30));
            Assert.IsNull(expireResult.Item1);
            Assert.IsTrue(expireResult.Item2);
        }

        [TestMethod]
        public void SetTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 3);

            var result = repo.SetAdd(new MemberPoint
            {
                MemberId = 100000,
                Point = 10000
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.SetAdd(new MemberPoint
            {
                MemberId = 100000,
                Point = 10001
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.SetAdd(new MemberPoint
            {
                MemberId = 100000,
                Point = 10002
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            result = repo.SetAdd(new MemberPoint
            {
                MemberId = 100000,
                Point = 10002
            });

            Assert.IsNull(result.Item1);
            Assert.AreNotEqual(result.Item2, -1);

            var setResult = repo.SetAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100001, Point = 10001 },
                new MemberPoint { MemberId = 100001, Point = 10002 },
                new MemberPoint { MemberId = 100001, Point = 10007 },
            });

            Assert.IsNull(setResult.Item1);
            Assert.AreNotEqual(setResult.Item2, -1);

            var setCombineIntersect = repo.SetCombineIntersect(100000, 100001);

            Assert.IsNull(setCombineIntersect.Item1);
            Assert.IsNotNull(setCombineIntersect.Item2);

            var setCombineUnion = repo.SetCombineUnion(100000, 100001);

            Assert.IsNull(setCombineUnion.Item1);
            Assert.IsNotNull(setCombineUnion.Item2);

            var setCombineDifference = repo.SetCombineDifference(100000, 100001);

            Assert.IsNull(setCombineDifference.Item1);
            Assert.IsNotNull(setCombineDifference.Item2);

            setCombineDifference = repo.SetCombineDifference(100001, 100000);

            Assert.IsNull(setCombineDifference.Item1);
            Assert.IsNotNull(setCombineDifference.Item2);

            setResult = repo.SetAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100002, Point = 10001 },
                new MemberPoint { MemberId = 100002, Point = 10007 },
                new MemberPoint { MemberId = 100002, Point = 10009 },
            });

            Assert.IsNull(setResult.Item1);
            Assert.AreNotEqual(setResult.Item2, -1);


            setCombineIntersect = repo.SetCombineIntersect(new int[] { 100000, 100001, 100002 });

            Assert.IsNull(setCombineIntersect.Item1);
            Assert.IsNotNull(setCombineIntersect.Item2);

            setCombineUnion = repo.SetCombineUnion(new int[] { 100000, 100001, 100002 });

            Assert.IsNull(setCombineUnion.Item1);
            Assert.IsNotNull(setCombineUnion.Item2);

            setCombineDifference = repo.SetCombineDifference(new int[] { 100000, 100001, 100002 });

            Assert.IsNull(setCombineDifference.Item1);
            Assert.IsNotNull(setCombineDifference.Item2);

            setCombineDifference = repo.SetCombineDifference(new int[] { 100001, 100002, 100000 });

            Assert.IsNull(setCombineDifference.Item1);
            Assert.IsNotNull(setCombineDifference.Item2);

            setCombineDifference = repo.SetCombineDifference(new int[] { 100002, 100000, 100001 });

            Assert.IsNull(setCombineDifference.Item1);
            Assert.IsNotNull(setCombineDifference.Item2);

            //var setCombineIntersectAndStore = repo.SetCombineIntersectAndStore(100000, 100001);

            //Assert.IsNull(setCombineIntersectAndStore.Item1);
            //Assert.IsNotNull(setCombineIntersectAndStore.Item2);

            //var setCombineUnionAndStore = repo.SetCombineUnionAndStore(100000, 100001);

            //Assert.IsNull(setCombineUnionAndStore.Item1);
            //Assert.IsNotNull(setCombineUnionAndStore.Item2);

            //var setCombineDifferenceAndStore = repo.SetCombineDifferenceAndStore(100000, 100001);

            //Assert.IsNull(setCombineDifferenceAndStore.Item1);
            //Assert.IsNotNull(setCombineDifferenceAndStore.Item2);

            //setCombineDifferenceAndStore = repo.SetCombineDifferenceAndStore(100001, 100000);

            //Assert.IsNull(setCombineDifferenceAndStore.Item1);
            //Assert.IsNotNull(setCombineDifferenceAndStore.Item2);

            //var setCombineIntersectAndStore = repo.SetCombineIntersectAndStore(new int[] { 100000, 100001, 100002 });

            //Assert.IsNull(setCombineIntersectAndStore.Item1);
            //Assert.IsNotNull(setCombineIntersectAndStore.Item2);

            //var setCombineUnionAndStore = repo.SetCombineUnionAndStore(new int[] { 100000, 100001, 100002 });

            //Assert.IsNull(setCombineUnionAndStore.Item1);
            //Assert.IsNotNull(setCombineUnionAndStore.Item2);

            //var setCombineDifferenceAndStore = repo.SetCombineDifferenceAndStore(new int[] { 100000, 100001, 100002 });

            //Assert.IsNull(setCombineDifferenceAndStore.Item1);
            //Assert.IsNotNull(setCombineDifferenceAndStore.Item2);

            //setCombineDifferenceAndStore = repo.SetCombineDifferenceAndStore(new int[] { 100001, 100002, 100000 });

            //Assert.IsNull(setCombineDifferenceAndStore.Item1);
            //Assert.IsNotNull(setCombineDifferenceAndStore.Item2);

            //setCombineDifferenceAndStore = repo.SetCombineDifferenceAndStore(new int[] { 100002, 100000, 100001 });

            //Assert.IsNull(setCombineDifferenceAndStore.Item1);
            //Assert.IsNotNull(setCombineDifferenceAndStore.Item2);

            //var containResult = repo.SetContains(new MemberPoint { MemberId = 100002, Point = 10001 });

            //Assert.IsNull(containResult.Item1);
            //Assert.IsTrue(containResult.Item2);

            //containResult = repo.SetContains(new MemberPoint { MemberId = 100002, Point = 11001 });

            //Assert.IsNull(containResult.Item1);
            //Assert.IsFalse(containResult.Item2);

            var setMembers = repo.SetGetMembers(100002);

            Assert.IsNull(setMembers.Item1);
            Assert.IsNotNull(setMembers.Item2);


            var setMove = repo.SetMove(100001, 100002, 10002);

            Assert.IsNull(setMove.Item1);
            Assert.IsNotNull(setMove.Item2);

            var setPop = repo.SetPop(100002);

            Assert.IsNull(setPop.Item1);
            Assert.IsNotNull(setPop.Item2);

            var setRandomMember = repo.SetGetRandomMember(100002);

            Assert.IsNull(setRandomMember.Item1);
            Assert.IsNotNull(setRandomMember.Item2);

            var setGetRandomMember = repo.SetGetRandomMember(100002, 2);

            Assert.IsNull(setGetRandomMember.Item1);
            Assert.IsNotNull(setGetRandomMember.Item2);

            var setRemove = repo.SetRemove(new MemberPoint { MemberId = 100001, Point = 10001 });

            Assert.IsNull(setRemove.Item1);
            Assert.IsNotNull(setRemove.Item2);

            var setRemoves = repo.SetRemove(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100002, Point = 10001 },
                new MemberPoint { MemberId = 100002, Point = 10007 },
                new MemberPoint { MemberId = 100002, Point = 10009 },
            });

            Assert.IsNull(setRemoves.Item1);
            Assert.IsNotNull(setRemoves.Item2);

            // 怪怪的:沒有依照pageSize返回相對應數量的資料
            var setScan = repo.SetScan(new MemberPoint { MemberId = 100000, Point = 1000 }, 1);

            Assert.IsNull(setScan.Item1);
            Assert.IsNotNull(setScan.Item2);

            setScan = repo.SetScan(new MemberPoint { MemberId = 100000, Point = 1000 }, 2, 0);

            Assert.IsNull(setScan.Item1);
            Assert.IsNotNull(setScan.Item2);


        }
        [TestMethod]
        public void SortedSetTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 4);
            //var sortedSetAdd = repo.SortedSetAdd(new MemberPoint { MemberId = 100000, Point = 1000 });

            //Assert.IsNull(sortedSetAdd.Item1);
            //Assert.IsTrue(sortedSetAdd.Item2);

            var sortedSetAddArray = repo.SortedSetAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100000, Point = 1001 },
                new MemberPoint { MemberId = 100000, Point = 1002 },
                new MemberPoint { MemberId = 100000, Point = 1003 },
                new MemberPoint { MemberId = 100000, Point = 1000 },
                new MemberPoint { MemberId = 100000, Point = 100 },
                new MemberPoint { MemberId = 100000, Point = 1006 },
                new MemberPoint { MemberId = 100000, Point = 1007 },
                new MemberPoint { MemberId = 100000, Point = 1008 },
                new MemberPoint { MemberId = 100000, Point = 1009 },
                new MemberPoint { MemberId = 100000, Point = 1010 },
            });

            Assert.IsNull(sortedSetAddArray.Item1);
            Assert.AreNotEqual(sortedSetAddArray.Item2, -1);

            sortedSetAddArray = repo.SortedSetAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100001, Point = 1001 },
                new MemberPoint { MemberId = 100001, Point = 1002 },
                new MemberPoint { MemberId = 100001, Point = 1003 },
                new MemberPoint { MemberId = 100001, Point = 1000 },
                new MemberPoint { MemberId = 100001, Point = 100 },
                new MemberPoint { MemberId = 100001, Point = 1006 },
                new MemberPoint { MemberId = 100001, Point = 1007 },
                new MemberPoint { MemberId = 100001, Point = 1008 },
                new MemberPoint { MemberId = 100001, Point = 1009 },
                new MemberPoint { MemberId = 100001, Point = 1010 },
            });

            Assert.IsNull(sortedSetAddArray.Item1);
            Assert.AreNotEqual(sortedSetAddArray.Item2, -1);

            // 加總，沒有Difference
            var combineAndStore = repo.SortedSetCombineAndStoreIntersect(100000, 100001);

            Assert.IsNull(combineAndStore.Item1);
            Assert.AreNotEqual(combineAndStore.Item2, -1);

            combineAndStore = repo.SortedSetCombineAndStoreUnion(100000, 100001);

            Assert.IsNull(combineAndStore.Item1);
            Assert.AreNotEqual(combineAndStore.Item2, -1);

            combineAndStore = repo.SortedSetCombineAndStoreIntersect(new int[] { 100000, 100001 });

            Assert.IsNull(combineAndStore.Item1);
            Assert.AreNotEqual(combineAndStore.Item2, -1);

            combineAndStore = repo.SortedSetCombineAndStoreUnion(new int[] { 100000, 100001 });

            Assert.IsNull(combineAndStore.Item1);
            Assert.AreNotEqual(combineAndStore.Item2, -1);

            var sortedSetDecrement = repo.SortedSetDecrement(new MemberPoint { MemberId = 100001, Point = 1001 }, 100);

            Assert.IsNull(sortedSetDecrement.Item1);
            Assert.IsNotNull(sortedSetDecrement.Item2);

            var sortedSetIncrement = repo.SortedSetIncrement(new MemberPoint { MemberId = 100001, Point = 1001 }, 20000);

            Assert.IsNull(sortedSetIncrement.Item1);
            Assert.IsNotNull(sortedSetIncrement.Item2);

            var sortedSetLength = repo.SortedSetLength(100001);

            Assert.IsNull(sortedSetLength.Item1);
            Assert.IsNotNull(sortedSetLength.Item2);

            var sortedSetLengthByValue = repo.SortedSetLengthByValue(100001, "100", "1002");

            Assert.IsNull(sortedSetLengthByValue.Item1);
            Assert.IsNotNull(sortedSetLengthByValue.Item2);

            var sortedSetPop = repo.SortedSetPop(100001);

            Assert.IsNull(sortedSetPop.Item1);
            Assert.IsNotNull(sortedSetPop.Item2);

            var sortedSetPopArray = repo.SortedSetPop(100001, 2);

            Assert.IsNull(sortedSetPopArray.Item1);
            Assert.IsNotNull(sortedSetPopArray.Item2);

            // start和end是Index，0 base
            var sortedSetRangeByRank = repo.SortedSetRangeByRank(100000, 0, 3);

            Assert.IsNull(sortedSetRangeByRank.Item1);
            Assert.IsNotNull(sortedSetRangeByRank.Item2);
            
            // start和end是Index，0 base
            var sortedSetRangeByRankWithScores = repo.SortedSetRangeByRankWithScores(100000, 0, 3);

            Assert.IsNull(sortedSetRangeByRankWithScores.Item1);
            Assert.IsNotNull(sortedSetRangeByRankWithScores.Item2);

            var sortedSetRangeByScore = repo.SortedSetRangeByScore(100000, 700, 7007);

            Assert.IsNull(sortedSetRangeByScore.Item1);
            Assert.IsNotNull(sortedSetRangeByScore.Item2);

            var sortedSetRangeByScoreWithScores = repo.SortedSetRangeByScoreWithScores(100000, 700, 7007);

            Assert.IsNull(sortedSetRangeByScoreWithScores.Item1);
            Assert.IsNotNull(sortedSetRangeByScoreWithScores.Item2);

            var sortedSetRangeByValue = repo.SortedSetRangeByValue(100000);

            Assert.IsNull(sortedSetRangeByValue.Item1);
            Assert.IsNotNull(sortedSetRangeByValue.Item2);

            sortedSetRangeByValue = repo.SortedSetRangeByValue(100000, "1000", "1002");

            Assert.IsNull(sortedSetRangeByValue.Item1);
            Assert.IsNotNull(sortedSetRangeByValue.Item2);

            // 回傳Index = 排行，0 base
            var sortedSetRank = repo.SortedSetRank(100000, "100");

            Assert.IsNull(sortedSetRank.Item1);
            Assert.IsNotNull(sortedSetRank.Item2);

            var sortedSetRemove = repo.SortedSetRemove(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100000, Point = 1001 },
                new MemberPoint { MemberId = 100000, Point = 1002 }
            });

            Assert.IsNull(sortedSetRemove.Item1);
            Assert.IsNotNull(sortedSetRemove.Item2);

            var sortedSetRemoveOne = repo.SortedSetRemove(new MemberPoint { MemberId = 100000, Point = 1003 });

            Assert.IsNull(sortedSetRemoveOne.Item1);
            Assert.IsNotNull(sortedSetRemoveOne.Item2);

            var sortedSetRemoveRangeByRank = repo.SortedSetRemoveRangeByRank(100000, 1, 3);

            Assert.IsNull(sortedSetRemoveRangeByRank.Item1);
            Assert.IsNotNull(sortedSetRemoveRangeByRank.Item2);

            var sortedSetRemoveRangeByScore = repo.SortedSetRemoveRangeByScore(100000, 7056, 7070);

            Assert.IsNull(sortedSetRemoveRangeByScore.Item1);
            Assert.IsNotNull(sortedSetRemoveRangeByScore.Item2);

            var sortedSetRemoveRangeByValue = repo.SortedSetRemoveRangeByValue(100001, "1006", "1007");

            Assert.IsNull(sortedSetRemoveRangeByValue.Item1);
            Assert.IsNotNull(sortedSetRemoveRangeByValue.Item2);

            var sortedSetScan = repo.SortedSetScan(100001, "100*", 2);

            Assert.IsNull(sortedSetScan.Item1);
            Assert.IsNotNull(sortedSetScan.Item2);

            sortedSetScan = repo.SortedSetScan(100000, "100*", 2, 0);

            Assert.IsNull(sortedSetScan.Item1);
            Assert.IsNotNull(sortedSetScan.Item2);

            var sortedSetScore = repo.SortedSetScore(100001, "1008");

            Assert.IsNull(sortedSetScore.Item1);
            Assert.IsNotNull(sortedSetScore.Item2);
        }

        [TestMethod]
        public void HyperLogLogTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 5);
            var hyperLogLogAdd = repo.HyperLogLogAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100000, Point = 1001 },
                new MemberPoint { MemberId = 100000, Point = 1002 },
                new MemberPoint { MemberId = 100000, Point = 1003 },
                new MemberPoint { MemberId = 100000, Point = 1000 },
                new MemberPoint { MemberId = 100000, Point = 100 },
                new MemberPoint { MemberId = 100000, Point = 1006 },
                new MemberPoint { MemberId = 100000, Point = 1007 },
                new MemberPoint { MemberId = 100000, Point = 1008 },
                new MemberPoint { MemberId = 100000, Point = 1009 },
                new MemberPoint { MemberId = 100000, Point = 1010 },
            });

            Assert.IsNull(hyperLogLogAdd.Item1);
            Assert.IsNotNull(hyperLogLogAdd.Item2);

            hyperLogLogAdd = repo.HyperLogLogAdd(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100001, Point = 1001 },
                new MemberPoint { MemberId = 100001, Point = 1002 },
                new MemberPoint { MemberId = 100001, Point = 1003 },
                new MemberPoint { MemberId = 100001, Point = 1000 },
                new MemberPoint { MemberId = 100001, Point = 100 },
                new MemberPoint { MemberId = 100001, Point = 1006 },
                new MemberPoint { MemberId = 100001, Point = 1007 },
                new MemberPoint { MemberId = 100001, Point = 1008 },
                new MemberPoint { MemberId = 100001, Point = 1009 },
                new MemberPoint { MemberId = 100001, Point = 1010 },
                new MemberPoint { MemberId = 100001, Point = 1011 },
                new MemberPoint { MemberId = 100001, Point = 1012 },
            });

            Assert.IsNull(hyperLogLogAdd.Item1);
            Assert.IsNotNull(hyperLogLogAdd.Item2);

            hyperLogLogAdd = repo.HyperLogLogAdd(new MemberPoint { MemberId = 100000, Point = 1100 });

            Assert.IsNull(hyperLogLogAdd.Item1);
            Assert.IsNotNull(hyperLogLogAdd.Item2);

            var hyperLogLogLength = repo.HyperLogLogLength(100000);

            Assert.IsNull(hyperLogLogLength.Item1);
            Assert.IsNotNull(hyperLogLogLength.Item2);

            hyperLogLogLength = repo.HyperLogLogLength(new int[] { 100000, 100001 });

            Assert.IsNull(hyperLogLogLength.Item1);
            Assert.IsNotNull(hyperLogLogLength.Item2);

            var hyperLogLogMerge = repo.HyperLogLogMerge(100002, 100000, 100001);

            Assert.IsNull(hyperLogLogMerge.Item1);
            Assert.IsNotNull(hyperLogLogMerge.Item2);

            hyperLogLogMerge = repo.HyperLogLogMerge(100003, new int[] { 100000, 100001 });

            Assert.IsNull(hyperLogLogMerge.Item1);
            Assert.IsNotNull(hyperLogLogMerge.Item2);

            hyperLogLogLength = repo.HyperLogLogLength(100002);

            Assert.IsNull(hyperLogLogLength.Item1);
            Assert.IsNotNull(hyperLogLogLength.Item2);
        }

        [TestMethod]
        public void TransactionTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 6);

            repo.StringInsert(new MemberPoint { MemberId = 100000, Point = 1001 });
            repo.StringInsert(new MemberPoint { MemberId = 100001, Point = 1006 });

            var transactionStringSet = repo.TransactionStringSet(new List<MemberPoint>
            {
                new MemberPoint { MemberId = 100000, Point = 1001 },
                new MemberPoint { MemberId = 100001, Point = 1006 }
            }, 123456);

            Assert.IsNull(transactionStringSet.Item1);
            Assert.IsTrue(transactionStringSet.Item2);
        }


        [TestMethod]
        public void LuaTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 7);

            var luaAdd = repo.LuaAddIfNotExist(new MemberPoint { MemberId = 100000, Point = 1001 });

            Assert.IsNull(luaAdd.Item1);
            Assert.IsTrue(luaAdd.Item2);

            var luaUpdate = repo.LuaUpdateIfExist(new MemberPoint { MemberId = 100000, Point = 20000 });

            Assert.IsNull(luaUpdate.Item1);
            Assert.IsTrue(luaUpdate.Item2);
        }

        [TestMethod]
        public void LockTest()
        {
            IMemberPointRepository repo = new RedisMemberPointRepository(redis, 0);

            var lockGet = repo.LockStringGet(100002);

            Assert.IsNull(lockGet.Item1);
            Assert.IsTrue(lockGet.Item2);
        }
    }
}
