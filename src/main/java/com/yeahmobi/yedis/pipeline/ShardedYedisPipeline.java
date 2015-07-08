package com.yeahmobi.yedis.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.shard.ShardingStrategy;

public class ShardedYedisPipeline {

    private HashMap<GroupYedis, PipelineInfo> pipelineInfos = new HashMap<GroupYedis, PipelineInfo>();

    private ShardingStrategy                  shardingStrategy;

    private List<ResponseInfo>                responseInfos = new ArrayList<ResponseInfo>();

    static class PipelineInfo {

        YedisPipeline yedisPipeline;
        int           indexCount = 0;
    }

    static class ResponseInfo {

        // indexCount指向当前pipeline的response的位置
        YedisPipeline yedisPipeline;
        // indexCount指向当前pipeline的response的位置
        int           index;

        public ResponseInfo(YedisPipeline yedisPipeline, int index) {
            super();
            this.yedisPipeline = yedisPipeline;
            this.index = index;
        }
    }

    public ShardedYedisPipeline(ShardingStrategy shardingStrategy) {
        super();
        this.shardingStrategy = shardingStrategy;
    }

    private YedisPipeline route(byte[] key) {
        GroupYedis groupYedis = shardingStrategy.route(key);
        return getAndPut(groupYedis);
    }

    private YedisPipeline route(String key) {
        GroupYedis groupYedis = shardingStrategy.route(key);
        return getAndPut(groupYedis);
    }

    private YedisPipeline getAndPut(GroupYedis groupYedis) {
        PipelineInfo pipelineInfo = pipelineInfos.get(groupYedis);
        if (pipelineInfo == null) {
            pipelineInfo = new PipelineInfo();
            pipelineInfo.yedisPipeline = groupYedis.pipelined();
            pipelineInfos.put(groupYedis, pipelineInfo);
        }

        ResponseInfo responseInfo = new ResponseInfo(pipelineInfo.yedisPipeline, pipelineInfo.indexCount);
        responseInfos.add(responseInfo);

        pipelineInfo.indexCount++;

        return pipelineInfo.yedisPipeline;
    }

    public void sync() {
        List<String> errorMsg = new ArrayList<String>();
        for (PipelineInfo pipelineInfo : pipelineInfos.values()) {
            YedisPipeline pipeline = pipelineInfo.yedisPipeline;
            try {
                pipeline.sync();
            } catch (Exception e) {
                // 如果某个YedisPipeline 有异常，记录起来
                errorMsg.add(String.format("Pipeline(%s:%s) error message is " + e.getMessage(),
                                           pipeline.getConfig().getHost(), pipeline.getConfig().getPort()));
            }
        }
        if (errorMsg.size() > 0) {
            throw new RuntimeException("Some pipelines error, detail is: " + errorMsg.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public List<Object> syncAndReturnAll() {
        HashMap<YedisPipeline, Object> pipeline2ResponseList = new HashMap<YedisPipeline, Object>();
        for (PipelineInfo pipelineInfo : pipelineInfos.values()) {
            YedisPipeline pipeline = pipelineInfo.yedisPipeline;
            try {
                List<Object> responseList = pipeline.syncAndReturnAll();
                pipeline2ResponseList.put(pipeline, responseList);
            } catch (Exception e) {
                // 如果某个YedisPipeline 有异常，该pipeline就没有 responseList,只有 e
                pipeline2ResponseList.put(pipeline, e);
            }
        }
        List<Object> list = new ArrayList<Object>();
        for (ResponseInfo responseInfo : responseInfos) {
            Object object = pipeline2ResponseList.get(responseInfo.yedisPipeline);
            if (object instanceof List) {
                List<Object> responseList = (List<Object>) object;
                Object response = (responseList != null) ? responseList.get(responseInfo.index) : null;
                list.add(response);
            } else {
                // 如果某个YedisPipeline 有异常，该pipeline就没有 responseList,只有 e
                list.add(object);
            }
        }
        return list;
    }

    public Response<Long> append(String key, String value) {
        return route(key).append(key, value);
    }

    public Response<Long> append(byte[] key, byte[] value) {
        return route(key).append(key, value);
    }

    public Response<List<String>> blpop(String key) {
        return route(key).blpop(key);
    }

    public Response<List<String>> brpop(String key) {
        return route(key).brpop(key);
    }

    public Response<List<byte[]>> blpop(byte[] key) {
        return route(key).blpop(key);
    }

    public Response<List<byte[]>> brpop(byte[] key) {
        return route(key).brpop(key);
    }

    public Response<Long> decr(String key) {
        return route(key).decr(key);
    }

    public Response<Long> decr(byte[] key) {
        return route(key).decr(key);
    }

    public Response<Long> decrBy(String key, long integer) {
        return route(key).decrBy(key, integer);
    }

    public Response<Long> decrBy(byte[] key, long integer) {
        return route(key).decrBy(key, integer);
    }

    public Response<Long> del(String key) {
        return route(key).del(key);
    }

    public Response<Long> del(byte[] key) {
        return route(key).del(key);
    }

    public Response<Boolean> exists(String key) {
        return route(key).exists(key);
    }

    public Response<Boolean> exists(byte[] key) {
        return route(key).exists(key);
    }

    public Response<Long> expire(String key, int seconds) {
        return route(key).expire(key, seconds);
    }

    public Response<Long> expire(byte[] key, int seconds) {
        return route(key).expire(key, seconds);
    }

    public Response<Long> expireAt(String key, long unixTime) {
        return route(key).expireAt(key, unixTime);
    }

    public Response<Long> expireAt(byte[] key, long unixTime) {
        return route(key).expireAt(key, unixTime);
    }

    public Response<String> get(String key) {
        return route(key).get(key);
    }

    public Response<byte[]> get(byte[] key) {
        return route(key).get(key);
    }

    public Response<Boolean> getbit(String key, long offset) {
        return route(key).getbit(key, offset);
    }

    public Response<Boolean> getbit(byte[] key, long offset) {
        return route(key).getbit(key, offset);
    }

    public Response<Long> bitpos(String key, boolean value) {
        return route(key).bitpos(key, value);
    }

    public Response<Long> bitpos(String key, boolean value, BitPosParams params) {
        return route(key).bitpos(key, value, params);
    }

    public Response<Long> bitpos(byte[] key, boolean value) {
        return route(key).bitpos(key, value);
    }

    public Response<Long> bitpos(byte[] key, boolean value, BitPosParams params) {
        return route(key).bitpos(key, value, params);
    }

    public Response<String> getrange(String key, long startOffset, long endOffset) {
        return route(key).getrange(key, startOffset, endOffset);
    }

    public Response<String> getSet(String key, String value) {
        return route(key).getSet(key, value);
    }

    public Response<byte[]> getSet(byte[] key, byte[] value) {
        return route(key).getSet(key, value);
    }

    public Response<Long> getrange(byte[] key, long startOffset, long endOffset) {
        return route(key).getrange(key, startOffset, endOffset);
    }

    public Response<Long> hdel(String key, String... field) {
        return route(key).hdel(key, field);
    }

    public Response<Long> hdel(byte[] key, byte[]... field) {
        return route(key).hdel(key, field);
    }

    public Response<Long> sort(String key, SortingParams sortingParameters, String dstkey) {
        return route(key).sort(key, sortingParameters, dstkey);
    }

    public Response<Boolean> hexists(String key, String field) {
        return route(key).hexists(key, field);
    }

    public Response<Boolean> hexists(byte[] key, byte[] field) {
        return route(key).hexists(key, field);
    }

    public Response<Long> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return route(key).sort(key, sortingParameters, dstkey);
    }

    public Response<String> hget(String key, String field) {
        return route(key).hget(key, field);
    }

    public Response<Long> sort(String key, String dstkey) {
        return route(key).sort(key, dstkey);
    }

    public Response<byte[]> hget(byte[] key, byte[] field) {
        return route(key).hget(key, field);
    }

    public Response<Long> sort(byte[] key, byte[] dstkey) {
        return route(key).sort(key, dstkey);
    }

    public Response<Map<String, String>> hgetAll(String key) {
        return route(key).hgetAll(key);
    }

    public Response<Map<byte[], byte[]>> hgetAll(byte[] key) {
        return route(key).hgetAll(key);
    }

    public Response<Long> hincrBy(String key, String field, long value) {
        return route(key).hincrBy(key, field, value);
    }

    public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
        return route(key).hincrBy(key, field, value);
    }

    public Response<Set<String>> hkeys(String key) {
        return route(key).hkeys(key);
    }

    public Response<Set<byte[]>> hkeys(byte[] key) {
        return route(key).hkeys(key);
    }

    public Response<Long> hlen(String key) {
        return route(key).hlen(key);
    }

    public Response<Long> hlen(byte[] key) {
        return route(key).hlen(key);
    }

    public Response<List<String>> hmget(String key, String... fields) {
        return route(key).hmget(key, fields);
    }

    public Response<List<byte[]>> hmget(byte[] key, byte[]... fields) {
        return route(key).hmget(key, fields);
    }

    public Response<String> hmset(String key, Map<String, String> hash) {
        return route(key).hmset(key, hash);
    }

    public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
        return route(key).hmset(key, hash);
    }

    public Response<Long> hset(String key, String field, String value) {
        return route(key).hset(key, field, value);
    }

    public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
        return route(key).hset(key, field, value);
    }

    public Response<Long> hsetnx(String key, String field, String value) {
        return route(key).hsetnx(key, field, value);
    }

    public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
        return route(key).hsetnx(key, field, value);
    }

    public Response<List<String>> hvals(String key) {
        return route(key).hvals(key);
    }

    public Response<List<byte[]>> hvals(byte[] key) {
        return route(key).hvals(key);
    }

    public Response<Long> incr(String key) {
        return route(key).incr(key);
    }

    public Response<Long> incr(byte[] key) {
        return route(key).incr(key);
    }

    public Response<Long> incrBy(String key, long integer) {
        return route(key).incrBy(key, integer);
    }

    public Response<Long> incrBy(byte[] key, long integer) {
        return route(key).incrBy(key, integer);
    }

    public Response<String> lindex(String key, long index) {
        return route(key).lindex(key, index);
    }

    public Response<byte[]> lindex(byte[] key, long index) {
        return route(key).lindex(key, index);
    }

    public Response<Long> linsert(String key, LIST_POSITION where, String pivot, String value) {
        return route(key).linsert(key, where, pivot, value);
    }

    public Response<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return route(key).linsert(key, where, pivot, value);
    }

    public Response<Long> llen(String key) {
        return route(key).llen(key);
    }

    public Response<Long> llen(byte[] key) {
        return route(key).llen(key);
    }

    public Response<String> lpop(String key) {
        return route(key).lpop(key);
    }

    public Response<byte[]> lpop(byte[] key) {
        return route(key).lpop(key);
    }

    public Response<Long> lpush(String key, String... string) {
        return route(key).lpush(key, string);
    }

    public Response<Long> lpush(byte[] key, byte[]... string) {
        return route(key).lpush(key, string);
    }

    public Response<Long> lpushx(String key, String... string) {
        return route(key).lpushx(key, string);
    }

    public Response<Long> lpushx(byte[] key, byte[]... bytes) {
        return route(key).lpushx(key, bytes);
    }

    public Response<List<String>> lrange(String key, long start, long end) {
        return route(key).lrange(key, start, end);
    }

    public Response<List<byte[]>> lrange(byte[] key, long start, long end) {
        return route(key).lrange(key, start, end);
    }

    public Response<Long> lrem(String key, long count, String value) {
        return route(key).lrem(key, count, value);
    }

    public Response<Long> lrem(byte[] key, long count, byte[] value) {
        return route(key).lrem(key, count, value);
    }

    public Response<String> lset(String key, long index, String value) {
        return route(key).lset(key, index, value);
    }

    public Response<String> lset(byte[] key, long index, byte[] value) {
        return route(key).lset(key, index, value);
    }

    public Response<String> ltrim(String key, long start, long end) {
        return route(key).ltrim(key, start, end);
    }

    public Response<String> ltrim(byte[] key, long start, long end) {
        return route(key).ltrim(key, start, end);
    }

    public Response<Long> move(String key, int dbIndex) {
        return route(key).move(key, dbIndex);
    }

    public Response<Long> move(byte[] key, int dbIndex) {
        return route(key).move(key, dbIndex);
    }

    public Response<Long> persist(String key) {
        return route(key).persist(key);
    }

    public Response<Long> persist(byte[] key) {
        return route(key).persist(key);
    }

    public Response<String> rpop(String key) {
        return route(key).rpop(key);
    }

    public Response<byte[]> rpop(byte[] key) {
        return route(key).rpop(key);
    }

    public Response<Long> rpush(String key, String... string) {
        return route(key).rpush(key, string);
    }

    public Response<Long> rpush(byte[] key, byte[]... string) {
        return route(key).rpush(key, string);
    }

    public Response<Long> rpushx(String key, String... string) {
        return route(key).rpushx(key, string);
    }

    public Response<Long> rpushx(byte[] key, byte[]... string) {
        return route(key).rpushx(key, string);
    }

    public Response<Long> sadd(String key, String... member) {
        return route(key).sadd(key, member);
    }

    public Response<Long> sadd(byte[] key, byte[]... member) {
        return route(key).sadd(key, member);
    }

    public Response<Long> scard(String key) {
        return route(key).scard(key);
    }

    public Response<Long> scard(byte[] key) {
        return route(key).scard(key);
    }

    public Response<String> set(String key, String value) {
        return route(key).set(key, value);
    }

    public Response<String> set(byte[] key, byte[] value) {
        return route(key).set(key, value);
    }

    public Response<Boolean> setbit(String key, long offset, boolean value) {
        return route(key).setbit(key, offset, value);
    }

    public Response<Boolean> setbit(byte[] key, long offset, byte[] value) {
        return route(key).setbit(key, offset, value);
    }

    public Response<String> setex(String key, int seconds, String value) {
        return route(key).setex(key, seconds, value);
    }

    public Response<String> setex(byte[] key, int seconds, byte[] value) {
        return route(key).setex(key, seconds, value);
    }

    public Response<Long> setnx(String key, String value) {
        return route(key).setnx(key, value);
    }

    public Response<Long> setnx(byte[] key, byte[] value) {
        return route(key).setnx(key, value);
    }

    public Response<Long> setrange(String key, long offset, String value) {
        return route(key).setrange(key, offset, value);
    }

    public Response<Long> setrange(byte[] key, long offset, byte[] value) {
        return route(key).setrange(key, offset, value);
    }

    public Response<Boolean> sismember(String key, String member) {
        return route(key).sismember(key, member);
    }

    public Response<Boolean> sismember(byte[] key, byte[] member) {
        return route(key).sismember(key, member);
    }

    public Response<Set<String>> smembers(String key) {
        return route(key).smembers(key);
    }

    public Response<Set<byte[]>> smembers(byte[] key) {
        return route(key).smembers(key);
    }

    public Response<List<String>> sort(String key) {
        return route(key).sort(key);
    }

    public Response<List<byte[]>> sort(byte[] key) {
        return route(key).sort(key);
    }

    public Response<List<String>> sort(String key, SortingParams sortingParameters) {
        return route(key).sort(key, sortingParameters);
    }

    public Response<List<byte[]>> sort(byte[] key, SortingParams sortingParameters) {
        return route(key).sort(key, sortingParameters);
    }

    public Response<String> spop(String key) {
        return route(key).spop(key);
    }

    public Response<byte[]> spop(byte[] key) {
        return route(key).spop(key);
    }

    public Response<String> srandmember(String key) {
        return route(key).srandmember(key);
    }

    public Response<List<String>> srandmember(String key, int count) {
        return route(key).srandmember(key, count);
    }

    public Response<byte[]> srandmember(byte[] key) {
        return route(key).srandmember(key);
    }

    public Response<List<byte[]>> srandmember(byte[] key, int count) {
        return route(key).srandmember(key, count);
    }

    public Response<Long> srem(String key, String... member) {
        return route(key).srem(key, member);
    }

    public Response<Long> srem(byte[] key, byte[]... member) {
        return route(key).srem(key, member);
    }

    public Response<Long> strlen(String key) {
        return route(key).strlen(key);
    }

    public Response<Long> strlen(byte[] key) {
        return route(key).strlen(key);
    }

    public Response<String> substr(String key, int start, int end) {
        return route(key).substr(key, start, end);
    }

    public Response<String> substr(byte[] key, int start, int end) {
        return route(key).substr(key, start, end);
    }

    public Response<Long> ttl(String key) {
        return route(key).ttl(key);
    }

    public Response<Long> ttl(byte[] key) {
        return route(key).ttl(key);
    }

    public Response<String> type(String key) {
        return route(key).type(key);
    }

    public Response<String> type(byte[] key) {
        return route(key).type(key);
    }

    public Response<Long> zadd(String key, double score, String member) {
        return route(key).zadd(key, score, member);
    }

    public Response<Long> zadd(String key, Map<String, Double> scoreMembers) {
        return route(key).zadd(key, scoreMembers);
    }

    public Response<Long> zadd(byte[] key, double score, byte[] member) {
        return route(key).zadd(key, score, member);
    }

    public Response<Long> zcard(String key) {
        return route(key).zcard(key);
    }

    public Response<Long> zcard(byte[] key) {
        return route(key).zcard(key);
    }

    public Response<Long> zcount(String key, double min, double max) {
        return route(key).zcount(key, min, max);
    }

    public Response<Long> zcount(String key, String min, String max) {
        return route(key).zcount(key, min, max);
    }

    public Response<Long> zcount(byte[] key, double min, double max) {
        return route(key).zcount(key, min, max);
    }

    public Response<Double> zincrby(String key, double score, String member) {
        return route(key).zincrby(key, score, member);
    }

    public Response<Double> zincrby(byte[] key, double score, byte[] member) {
        return route(key).zincrby(key, score, member);
    }

    public Response<Set<String>> zrange(String key, long start, long end) {
        return route(key).zrange(key, start, end);
    }

    public Response<Set<byte[]>> zrange(byte[] key, long start, long end) {
        return route(key).zrange(key, start, end);
    }

    public Response<Set<String>> zrangeByScore(String key, double min, double max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Response<Set<String>> zrangeByScore(String key, String min, String max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Response<Set<String>> zrangeByScore(String key, double min, double max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<String>> zrangeByScore(String key, String min, String max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<String>> zrevrangeByScore(String key, double max, double min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Response<Set<String>> zrevrangeByScore(String key, String max, String min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Response<Set<String>> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<String>> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrangeWithScores(String key, long start, long end) {
        return route(key).zrangeWithScores(key, start, end);
    }

    public Response<Set<Tuple>> zrangeWithScores(byte[] key, long start, long end) {
        return route(key).zrangeWithScores(key, start, end);
    }

    public Response<Long> zrank(String key, String member) {
        return route(key).zrank(key, member);
    }

    public Response<Long> zrank(byte[] key, byte[] member) {
        return route(key).zrank(key, member);
    }

    public Response<Long> zrem(String key, String... member) {
        return route(key).zrem(key, member);
    }

    public Response<Long> zrem(byte[] key, byte[]... member) {
        return route(key).zrem(key, member);
    }

    public Response<Long> zremrangeByRank(String key, long start, long end) {
        return route(key).zremrangeByRank(key, start, end);
    }

    public Response<Long> zremrangeByRank(byte[] key, long start, long end) {
        return route(key).zremrangeByRank(key, start, end);
    }

    public Response<Long> zremrangeByScore(String key, double start, double end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(String key, String start, String end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Response<Set<String>> zrevrange(String key, long start, long end) {
        return route(key).zrevrange(key, start, end);
    }

    public Response<Set<byte[]>> zrevrange(byte[] key, long start, long end) {
        return route(key).zrevrange(key, start, end);
    }

    public Response<Set<Tuple>> zrevrangeWithScores(String key, long start, long end) {
        return route(key).zrevrangeWithScores(key, start, end);
    }

    public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, long start, long end) {
        return route(key).zrevrangeWithScores(key, start, end);
    }

    public Response<Long> zrevrank(String key, String member) {
        return route(key).zrevrank(key, member);
    }

    public Response<Long> zrevrank(byte[] key, byte[] member) {
        return route(key).zrevrank(key, member);
    }

    public Response<Double> zscore(String key, String member) {
        return route(key).zscore(key, member);
    }

    public Response<Double> zscore(byte[] key, byte[] member) {
        return route(key).zscore(key, member);
    }

    public Response<Long> bitcount(String key) {
        return route(key).bitcount(key);
    }

    public Response<Long> bitcount(String key, long start, long end) {
        return route(key).bitcount(key, start, end);
    }

    public Response<Long> bitcount(byte[] key) {
        return route(key).bitcount(key);
    }

    public Response<Long> bitcount(byte[] key, long start, long end) {
        return route(key).bitcount(key, start, end);
    }

    public Response<byte[]> dump(String key) {
        return route(key).dump(key);
    }

    public Response<byte[]> dump(byte[] key) {
        return route(key).dump(key);
    }

    public Response<String> migrate(String host, int port, String key, int destinationDb, int timeout) {
        return route(key).migrate(host, port, key, destinationDb, timeout);
    }

    public Response<String> migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
        return route(key).migrate(host, port, key, destinationDb, timeout);
    }

    public Response<Long> objectRefcount(String key) {
        return route(key).objectRefcount(key);
    }

    public Response<Long> objectRefcount(byte[] key) {
        return route(key).objectRefcount(key);
    }

    public Response<String> objectEncoding(String key) {
        return route(key).objectEncoding(key);
    }

    public Response<byte[]> objectEncoding(byte[] key) {
        return route(key).objectEncoding(key);
    }

    public Response<Long> objectIdletime(String key) {
        return route(key).objectIdletime(key);
    }

    public Response<Long> objectIdletime(byte[] key) {
        return route(key).objectIdletime(key);
    }

    @Deprecated
    public Response<Long> pexpire(String key, int milliseconds) {
        return route(key).pexpire(key, milliseconds);
    }

    @Deprecated
    public Response<Long> pexpire(byte[] key, int milliseconds) {
        return route(key).pexpire(key, milliseconds);
    }

    public Response<Long> pexpire(String key, long milliseconds) {
        return route(key).pexpire(key, milliseconds);
    }

    public Response<Long> pexpire(byte[] key, long milliseconds) {
        return route(key).pexpire(key, milliseconds);
    }

    public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
        return route(key).pexpireAt(key, millisecondsTimestamp);
    }

    public Response<Long> pexpireAt(byte[] key, long millisecondsTimestamp) {
        return route(key).pexpireAt(key, millisecondsTimestamp);
    }

    public Response<Long> pttl(String key) {
        return route(key).pttl(key);
    }

    public Response<Long> pttl(byte[] key) {
        return route(key).pttl(key);
    }

    public Response<String> restore(String key, int ttl, byte[] serializedValue) {
        return route(key).restore(key, ttl, serializedValue);
    }

    public Response<String> restore(byte[] key, int ttl, byte[] serializedValue) {
        return route(key).restore(key, ttl, serializedValue);
    }

    public Response<Double> incrByFloat(String key, double increment) {
        return route(key).incrByFloat(key, increment);
    }

    public Response<Double> incrByFloat(byte[] key, double increment) {
        return route(key).incrByFloat(key, increment);
    }

    public Response<String> psetex(String key, int milliseconds, String value) {
        return route(key).psetex(key, milliseconds, value);
    }

    public Response<String> psetex(byte[] key, int milliseconds, byte[] value) {
        return route(key).psetex(key, milliseconds, value);
    }

    public Response<String> set(String key, String value, String nxxx) {
        return route(key).set(key, value, nxxx);
    }

    public Response<String> set(byte[] key, byte[] value, byte[] nxxx) {
        return route(key).set(key, value, nxxx);
    }

    public Response<String> set(String key, String value, String nxxx, String expx, int time) {
        return route(key).set(key, value, nxxx, expx, time);
    }

    public Response<String> set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
        return route(key).set(key, value, nxxx, expx, time);
    }

    public Response<Double> hincrByFloat(String key, String field, double increment) {
        return route(key).hincrByFloat(key, field, increment);
    }

    public Response<Double> hincrByFloat(byte[] key, byte[] field, double increment) {
        return route(key).hincrByFloat(key, field, increment);
    }

    public Response<Long> pfadd(byte[] key, byte[]... elements) {
        return route(key).pfadd(key, elements);
    }

    public Response<Long> pfcount(byte[] key) {
        return route(key).pfcount(key);
    }

    public Response<Long> pfadd(String key, String... elements) {
        return route(key).pfadd(key, elements);
    }

    public Response<Long> pfcount(String key) {
        return route(key).pfcount(key);
    }

}
