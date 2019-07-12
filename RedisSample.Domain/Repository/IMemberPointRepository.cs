namespace RedisSample.Domain.Repository
{
    using RedisSample.Domain.Model;
    using System;
    using System.Collections.Generic;
    public interface IMemberPointRepository
    {
        Tuple<Exception, bool> KeyDelete(int memberId);

        Tuple<Exception, bool> KeyExpire(int memberId, TimeSpan expire);

        //*********************************************************************************//
        Tuple<Exception, bool> StringInsert(MemberPoint memberPoint);

        Tuple<Exception, bool> StringInsert(MemberPoint memberPoint, TimeSpan expire);

        Tuple<Exception, bool> StringUpdate(MemberPoint memberPoint);

        Tuple<Exception, bool> StringUpdate(MemberPoint memberPoint, TimeSpan expire);

        Tuple<Exception, bool> StringIncrement(int memberId, long changePoint);

        Tuple<Exception, bool> StringDecrement(int memberId, long changePoint);

        Tuple<Exception, MemberPoint> StringFind(int member);

        Tuple<Exception, IEnumerable<MemberPoint>> StringGet(int[] members);
        //*********************************************************************************//
        Tuple<Exception, bool> HashInsert(MemberPoint memberPoint);

        Tuple<Exception, bool> HashUpdate(MemberPoint memberPoint);

        Tuple<Exception, bool> HashIncrement(int memberId, long changePoint);

        Tuple<Exception, bool> HashDecrement(int memberId, long changePoint);

        Tuple<Exception, MemberPoint> HashFindAll(int member);

        Tuple<Exception, MemberPoint> HashFindField(int member);

        Tuple<Exception, IEnumerable<MemberPoint>> HashBatchGet(int[] members);

        //*********************************************************************************//
        Tuple<Exception, long> ListLeftPush(MemberPoint memberPoint);

        Tuple<Exception, long> ListLeftPush(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, string> ListFindByIndex(int memberId, long index);

        Tuple<Exception, long> ListInsertAfter(MemberPoint indexMemberPoint, MemberPoint addMemberPoint);

        Tuple<Exception, long> ListInsertBefore(MemberPoint indexMemberPoint, MemberPoint addMemberPoint);

        Tuple<Exception, IEnumerable<MemberPoint>> ListRange(int memberId);

        Tuple<Exception, long> ListRemove(MemberPoint memberPoint);

        Tuple<Exception, MemberPoint> ListLeftPop(int memberId);

        Tuple<Exception, long> ListLength(int memberId);

        Tuple<Exception, MemberPoint> ListRightPopLeftPush(int sourceMember, int destinationMember);

        Tuple<Exception, long> ListRightPush(MemberPoint memberPoint);

        Tuple<Exception, long> ListRightPush(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, MemberPoint> ListRightPop(int memberId);

        //*********************************************************************************//

        Tuple<Exception, bool> SetAdd(MemberPoint memberPoint);

        Tuple<Exception, long> SetAdd(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineIntersect(int firstMember, int secondMember);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineUnion(int firstMember, int secondMember);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineDifference(int firstMember, int secondMember);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineIntersect(IEnumerable<int> members);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineUnion(IEnumerable<int> members);

        Tuple<Exception, IEnumerable<MemberPoint>> SetCombineDifference(IEnumerable<int> members);

        Tuple<Exception, long> SetCombineIntersectAndStore(int firstMember, int secondMember);

        Tuple<Exception, long> SetCombineUnionAndStore(int firstMember, int secondMember);

        Tuple<Exception, long> SetCombineDifferenceAndStore(int firstMember, int secondMember);

        Tuple<Exception, long> SetCombineIntersectAndStore(IEnumerable<int> members);

        Tuple<Exception, long> SetCombineUnionAndStore(IEnumerable<int> members);

        Tuple<Exception, long> SetCombineDifferenceAndStore(IEnumerable<int> members);

        Tuple<Exception, bool> SetContains(MemberPoint memberPoint);

        Tuple<Exception, IEnumerable<MemberPoint>> SetGetMembers(int memberId);

        Tuple<Exception, bool> SetMove(int source, int destination, long point);

        Tuple<Exception, MemberPoint> SetPop(int memberId);

        Tuple<Exception, IEnumerable<MemberPoint>> SetPop(int memberId, int count);

        Tuple<Exception, MemberPoint> SetGetRandomMember(int memberId);

        Tuple<Exception, IEnumerable<MemberPoint>> SetGetRandomMember(int memberId, int count);

        Tuple<Exception, bool> SetRemove(MemberPoint memberPoint);

        Tuple<Exception, long> SetRemove(IEnumerable<MemberPoint> members);

        Tuple<Exception, IEnumerable<MemberPoint>> SetScan(MemberPoint memberPoint, int pageSize);

        Tuple<Exception, IEnumerable<MemberPoint>> SetScan(MemberPoint memberPoint, int pageSize, int pageOffset);

        //*********************************************************************************//
        Tuple<Exception, bool> SortedSetAdd(MemberPoint memberPoint);

        Tuple<Exception, long> SortedSetAdd(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, long> SortedSetCombineAndStoreIntersect(int firstMember, int secondMember);

        Tuple<Exception, long> SortedSetCombineAndStoreUnion(int firstMember, int secondMember);

        Tuple<Exception, long> SortedSetCombineAndStoreIntersect(IEnumerable<int> members);

        Tuple<Exception, long> SortedSetCombineAndStoreUnion(IEnumerable<int> members);

        Tuple<Exception, MemberPoint> SortedSetDecrement(MemberPoint memberPoint, long changePoint);

        Tuple<Exception, MemberPoint> SortedSetIncrement(MemberPoint memberPoint, long changePoint);

        Tuple<Exception, long> SortedSetLength(int memberId);

        Tuple<Exception, long> SortedSetLengthByValue(int memberId, string start, string end);

        Tuple<Exception, MemberPoint> SortedSetPop(int memberId);

        Tuple<Exception, IEnumerable<MemberPoint>> SortedSetPop(int memberId, long count);

        Tuple<Exception, IEnumerable<string>> SortedSetRangeByRank(int memberId, long start, long stop);

        Tuple<Exception, IEnumerable<MemberPoint>> SortedSetRangeByRankWithScores(int memberId, long start, long stop);

        Tuple<Exception, IEnumerable<string>> SortedSetRangeByScore(int memberId, double start, double stop);

        Tuple<Exception, IEnumerable<MemberPoint>> SortedSetRangeByScoreWithScores(int memberId, double start, double stop);

        Tuple<Exception, IEnumerable<string>> SortedSetRangeByValue(int memberId);

        Tuple<Exception, IEnumerable<string>> SortedSetRangeByValue(int memberId, string start, string stop);

        Tuple<Exception, long> SortedSetRank(int memberId, string value);

        Tuple<Exception, long> SortedSetRemove(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, bool> SortedSetRemove(MemberPoint memberPoints);

        Tuple<Exception, long> SortedSetRemoveRangeByRank(int memberId, long start, long stop);

        Tuple<Exception, long> SortedSetRemoveRangeByScore(int memberId, double start, double stop);

        Tuple<Exception, long> SortedSetRemoveRangeByValue(int memberId, string start, string stop);

        Tuple<Exception, IEnumerable<MemberPoint>> SortedSetScan(int memberId, string pattern, int pageSize);

        Tuple<Exception, IEnumerable<MemberPoint>> SortedSetScan(int memberId, string pattern, int pageSize, int pageOffset);

        Tuple<Exception, double> SortedSetScore(int memberId, string value);

        //*********************************************************************************//
        Tuple<Exception, bool> HyperLogLogAdd(IEnumerable<MemberPoint> memberPoints);

        Tuple<Exception, bool> HyperLogLogAdd(MemberPoint memberPoint);

        Tuple<Exception, long> HyperLogLogLength(int memberId);

        Tuple<Exception, long> HyperLogLogLength(IEnumerable<int> members);

        Tuple<Exception, bool> HyperLogLogMerge(int destinationMember, int firstMember, int secondMember);

        Tuple<Exception, bool> HyperLogLogMerge(int destinationMember, IEnumerable<int> members);

        //*********************************************************************************//
        Tuple<Exception, bool> TransactionStringSet(IEnumerable<MemberPoint> memberPoints, long newPoint);

        //*********************************************************************************//
        Tuple<Exception, IEnumerable<MemberPoint>> StringBatchGet(int[] members);
        //*********************************************************************************//
        Tuple<Exception, bool> LuaAddIfNotExist(MemberPoint memberPoint);

        Tuple<Exception, bool> LuaUpdateIfExist(MemberPoint memberPoint);
    }
}
