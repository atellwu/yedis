package com.yeahmobi.yedis.shard;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.pipeline.ShardedYedisPipeline;

/**
 * @author Leo.Liang
 */
public class ShardedYedis {

    private ShardingStrategy shardingStrategy;

    public ShardedYedis(List<GroupYedis> groups, ShardingAlgorithm algo,
                        HashCodeComputingStrategy hashCodeComputingStrategy) {
        this.shardingStrategy = ShardingStrategyFactory.createShardingStrategy(groups, algo, hashCodeComputingStrategy);
    }

    public void close() {
        this.shardingStrategy.close();
    }

    private GroupYedis route(String key) {
        return shardingStrategy.route(key);
    }

    private GroupYedis route(byte[] key) {
        return shardingStrategy.route(key);
    }

    public ShardedYedisPipeline pipeline(){
        return new ShardedYedisPipeline(shardingStrategy);
    }

    public String set(byte[] key, byte[] value) {
        return route(key).set(key, value);
    }

    public byte[] get(byte[] key) {
        return route(key).get(key);
    }

    public Boolean exists(byte[] key) {
        return route(key).exists(key);
    }

    public Long persist(byte[] key) {
        return route(key).persist(key);
    }

    public String type(byte[] key) {
        return route(key).type(key);
    }

    public Long expire(byte[] key, int seconds) {
        return route(key).expire(key, seconds);
    }

    public Long expireAt(byte[] key, long unixTime) {
        return route(key).expireAt(key, unixTime);
    }

    public Long ttl(byte[] key) {
        return route(key).ttl(key);
    }

    public Boolean setbit(byte[] key, long offset, boolean value) {
        return route(key).setbit(key, offset, value);
    }

    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return route(key).setbit(key, offset, value);
    }

    public Boolean getbit(byte[] key, long offset) {
        return route(key).getbit(key, offset);
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        return route(key).setrange(key, offset, value);
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        return route(key).getrange(key, startOffset, endOffset);
    }

    public byte[] getSet(byte[] key, byte[] value) {
        return route(key).getSet(key, value);
    }

    public Long setnx(byte[] key, byte[] value) {
        return route(key).setnx(key, value);
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        return route(key).setex(key, seconds, value);
    }

    public Long decrBy(byte[] key, long integer) {
        return route(key).decrBy(key, integer);
    }

    public Long decr(byte[] key) {
        return route(key).decr(key);
    }

    public Long incrBy(byte[] key, long integer) {
        return route(key).incrBy(key, integer);
    }

    public Double incrByFloat(byte[] key, double value) {
        return route(key).incrByFloat(key, value);
    }

    public Long incr(byte[] key) {
        return route(key).incr(key);
    }

    public Long append(byte[] key, byte[] value) {
        return route(key).append(key, value);
    }

    public byte[] substr(byte[] key, int start, int end) {
        return route(key).substr(key, start, end);
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        return route(key).hset(key, field, value);
    }

    public byte[] hget(byte[] key, byte[] field) {
        return route(key).hget(key, field);
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return route(key).hsetnx(key, field, value);
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return route(key).hmset(key, hash);
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return route(key).hmget(key, fields);
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        return route(key).hincrBy(key, field, value);
    }

    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return route(key).hincrByFloat(key, field, value);
    }

    public Boolean hexists(byte[] key, byte[] field) {
        return route(key).hexists(key, field);
    }

    public Long hdel(byte[] key, byte[]... field) {
        return route(key).hdel(key, field);
    }

    public Long hlen(byte[] key) {
        return route(key).hlen(key);
    }

    public Set<byte[]> hkeys(byte[] key) {
        return route(key).hkeys(key);
    }

    public Collection<byte[]> hvals(byte[] key) {
        return route(key).hvals(key);
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return route(key).hgetAll(key);
    }

    public Long rpush(byte[] key, byte[]... args) {
        return route(key).rpush(key, args);
    }

    public Long lpush(byte[] key, byte[]... args) {
        return route(key).lpush(key, args);
    }

    public Long llen(byte[] key) {
        return route(key).llen(key);
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        return route(key).lrange(key, start, end);
    }

    public String ltrim(byte[] key, long start, long end) {
        return route(key).ltrim(key, start, end);
    }

    public byte[] lindex(byte[] key, long index) {
        return route(key).lindex(key, index);
    }

    public String lset(byte[] key, long index, byte[] value) {
        return route(key).lset(key, index, value);
    }

    public Long lrem(byte[] key, long count, byte[] value) {
        return route(key).lrem(key, count, value);
    }

    public byte[] lpop(byte[] key) {
        return route(key).lpop(key);
    }

    public byte[] rpop(byte[] key) {
        return route(key).rpop(key);
    }

    public Long sadd(byte[] key, byte[]... member) {
        return route(key).sadd(key, member);
    }

    public Set<byte[]> smembers(byte[] key) {
        return route(key).smembers(key);
    }

    public Long srem(byte[] key, byte[]... member) {
        return route(key).srem(key, member);
    }

    public byte[] spop(byte[] key) {
        return route(key).spop(key);
    }

    public Long scard(byte[] key) {
        return route(key).scard(key);
    }

    public Boolean sismember(byte[] key, byte[] member) {
        return route(key).sismember(key, member);
    }

    public byte[] srandmember(byte[] key) {
        return route(key).srandmember(key);
    }

    public Long strlen(byte[] key) {
        return route(key).strlen(key);
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        return route(key).zadd(key, score, member);
    }

    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return route(key).zadd(key, scoreMembers);
    }

    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return route(key).zrange(key, start, end);
    }

    public Long zrem(byte[] key, byte[]... member) {
        return route(key).zrem(key, member);
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        return route(key).zincrby(key, score, member);
    }

    public Long zrank(byte[] key, byte[] member) {
        return route(key).zrank(key, member);
    }

    public Long zrevrank(byte[] key, byte[] member) {
        return route(key).zrevrank(key, member);
    }

    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return route(key).zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return route(key).zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return route(key).zrevrangeWithScores(key, start, end);
    }

    public Long zcard(byte[] key) {
        return route(key).zcard(key);
    }

    public Double zscore(byte[] key, byte[] member) {
        return route(key).zscore(key, member);
    }

    public List<byte[]> sort(byte[] key) {
        return route(key).sort(key);
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return route(key).sort(key, sortingParameters);
    }

    public Long zcount(byte[] key, double min, double max) {
        return route(key).zcount(key, min, max);
    }

    public Long zcount(byte[] key, byte[] min, byte[] max) {
        return route(key).zcount(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Long zremrangeByRank(byte[] key, long start, long end) {
        return route(key).zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return route(key).linsert(key, where, pivot, value);
    }

    public Long lpushx(byte[] key, byte[]... arg) {
        return route(key).lpushx(key, arg);
    }

    public Long rpushx(byte[] key, byte[]... arg) {
        return route(key).rpushx(key, arg);
    }

    public List<byte[]> blpop(byte[] arg) {
        return route(arg).blpop(arg);
    }

    public List<byte[]> brpop(byte[] arg) {
        return route(arg).brpop(arg);
    }

    public Long del(byte[] key) {
        return route(key).del(key);
    }

    public byte[] echo(byte[] arg) {
        return route(arg).echo(arg);
    }

    public Long move(byte[] key, int dbIndex) {
        return route(key).move(key, dbIndex);
    }

    public Long bitcount(byte[] key) {
        return route(key).bitcount(key);
    }

    public Long bitcount(byte[] key, long start, long end) {
        return route(key).bitcount(key, start, end);
    }

    public Long pfadd(byte[] key, byte[]... elements) {
        return route(key).pfadd(key, elements);
    }

    public long pfcount(byte[] key) {
        return route(key).pfcount(key);
    }

    public String set(String key, String value) {
        return route(key).set(key, value);
    }

    public String set(String key, String value, String nxxx, String expx, long time) {
        return route(key).set(key, value, nxxx, expx, time);
    }

    public String get(String key) {
        return route(key).get(key);
    }

    public Boolean exists(String key) {
        return route(key).exists(key);
    }

    public Long persist(String key) {
        return route(key).persist(key);
    }

    public String type(String key) {
        return route(key).type(key);
    }

    public Long expire(String key, int seconds) {
        return route(key).expire(key, seconds);
    }

    public Long expireAt(String key, long unixTime) {
        return route(key).expireAt(key, unixTime);
    }

    public Long ttl(String key) {
        return route(key).ttl(key);
    }

    public Boolean setbit(String key, long offset, boolean value) {
        return route(key).setbit(key, offset, value);
    }

    public Boolean setbit(String key, long offset, String value) {
        return route(key).setbit(key, offset, value);
    }

    public Boolean getbit(String key, long offset) {
        return route(key).getbit(key, offset);
    }

    public Long setrange(String key, long offset, String value) {
        return route(key).setrange(key, offset, value);
    }

    public String getrange(String key, long startOffset, long endOffset) {
        return route(key).getrange(key, startOffset, endOffset);
    }

    public String getSet(String key, String value) {
        return route(key).getSet(key, value);
    }

    public Long setnx(String key, String value) {
        return route(key).setnx(key, value);
    }

    public String setex(String key, int seconds, String value) {
        return route(key).setex(key, seconds, value);
    }

    public Long decrBy(String key, long integer) {
        return route(key).decrBy(key, integer);
    }

    public Long decr(String key) {
        return route(key).decr(key);
    }

    public Long incrBy(String key, long integer) {
        return route(key).incrBy(key, integer);
    }

    public Long incr(String key) {
        return route(key).incr(key);
    }

    public Long append(String key, String value) {
        return route(key).append(key, value);
    }

    public String substr(String key, int start, int end) {
        return route(key).substr(key, start, end);
    }

    public Long hset(String key, String field, String value) {
        return route(key).hset(key, field, value);
    }

    public String hget(String key, String field) {
        return route(key).hget(key, field);
    }

    public Long hsetnx(String key, String field, String value) {
        return route(key).hsetnx(key, field, value);
    }

    public String hmset(String key, Map<String, String> hash) {
        return route(key).hmset(key, hash);
    }

    public List<String> hmget(String key, String... fields) {
        return route(key).hmget(key, fields);
    }

    public Long hincrBy(String key, String field, long value) {
        return route(key).hincrBy(key, field, value);
    }

    public Boolean hexists(String key, String field) {
        return route(key).hexists(key, field);
    }

    public Long hdel(String key, String... field) {
        return route(key).hdel(key, field);
    }

    public Long hlen(String key) {
        return route(key).hlen(key);
    }

    public Set<String> hkeys(String key) {
        return route(key).hkeys(key);
    }

    public List<String> hvals(String key) {
        return route(key).hvals(key);
    }

    public Map<String, String> hgetAll(String key) {
        return route(key).hgetAll(key);
    }

    public Long rpush(String key, String... string) {
        return route(key).rpush(key, string);
    }

    public Long lpush(String key, String... string) {
        return route(key).lpush(key, string);
    }

    public Long llen(String key) {
        return route(key).llen(key);
    }

    public List<String> lrange(String key, long start, long end) {
        return route(key).lrange(key, start, end);
    }

    public String ltrim(String key, long start, long end) {
        return route(key).ltrim(key, start, end);
    }

    public String lindex(String key, long index) {
        return route(key).lindex(key, index);
    }

    public String lset(String key, long index, String value) {
        return route(key).lset(key, index, value);
    }

    public Long lrem(String key, long count, String value) {
        return route(key).lrem(key, count, value);
    }

    public String lpop(String key) {
        return route(key).lpop(key);
    }

    public String rpop(String key) {
        return route(key).rpop(key);
    }

    public Long sadd(String key, String... member) {
        return route(key).sadd(key, member);
    }

    public Set<String> smembers(String key) {
        return route(key).smembers(key);
    }

    public Long srem(String key, String... member) {
        return route(key).srem(key, member);
    }

    public String spop(String key) {
        return route(key).spop(key);
    }

    public Long scard(String key) {
        return route(key).scard(key);
    }

    public Boolean sismember(String key, String member) {
        return route(key).sismember(key, member);
    }

    public String srandmember(String key) {
        return route(key).srandmember(key);
    }

    public Long strlen(String key) {
        return route(key).strlen(key);
    }

    public Long zadd(String key, double score, String member) {
        return route(key).zadd(key, score, member);
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return route(key).zadd(key, scoreMembers);
    }

    public Set<String> zrange(String key, long start, long end) {
        return route(key).zrange(key, start, end);
    }

    public Long zrem(String key, String... member) {
        return route(key).zrem(key, member);
    }

    public Double zincrby(String key, double score, String member) {
        return route(key).zincrby(key, score, member);
    }

    public Long zrank(String key, String member) {
        return route(key).zrank(key, member);
    }

    public Long zrevrank(String key, String member) {
        return route(key).zrevrank(key, member);
    }

    public Set<String> zrevrange(String key, long start, long end) {
        return route(key).zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return route(key).zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return route(key).zrevrangeWithScores(key, start, end);
    }

    public Long zcard(String key) {
        return route(key).zcard(key);
    }

    public Double zscore(String key, String member) {
        return route(key).zscore(key, member);
    }

    public List<String> sort(String key) {
        return route(key).sort(key);
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        return route(key).sort(key, sortingParameters);
    }

    public Long zcount(String key, double min, double max) {
        return route(key).zcount(key, min, max);
    }

    public Long zcount(String key, String min, String max) {
        return route(key).zcount(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<String> zrangeByScore(String key, String min, String max) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return route(key).zrevrangeByScore(key, max, min);
    }

    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return route(key).zrangeByScore(key, min, max);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return route(key).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return route(key).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return route(key).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return route(key).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return route(key).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Long zremrangeByRank(String key, long start, long end) {
        return route(key).zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(String key, double start, double end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Long zremrangeByScore(String key, String start, String end) {
        return route(key).zremrangeByScore(key, start, end);
    }

    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return route(key).linsert(key, where, pivot, value);
    }

    public Long lpushx(String key, String... string) {
        return route(key).lpushx(key, string);
    }

    public Long rpushx(String key, String... string) {
        return route(key).rpushx(key, string);
    }

    public List<String> blpop(String arg) {
        return route(arg).blpop(arg);
    }

    public List<String> brpop(String arg) {
        return route(arg).brpop(arg);
    }

    public Long del(String key) {
        return route(key).del(key);
    }

    public String echo(String string) {
        return route(string).echo(string);
    }

    public Long move(String key, int dbIndex) {
        return route(key).move(key, dbIndex);
    }

    public Long bitcount(String key) {
        return route(key).bitcount(key);
    }

    public Long bitcount(String key, long start, long end) {
        return route(key).bitcount(key, start, end);
    }

    @Deprecated
    public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
        return route(key).hscan(key, cursor);
    }

    @Deprecated
    public ScanResult<String> sscan(String key, int cursor) {
        return route(key).sscan(key, cursor);
    }

    @Deprecated
    public ScanResult<Tuple> zscan(String key, int cursor) {
        return route(key).zscan(key, cursor);
    }

    public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
        return route(key).hscan(key, cursor);
    }

    public ScanResult<String> sscan(String key, String cursor) {
        return route(key).sscan(key, cursor);
    }

    public ScanResult<Tuple> zscan(String key, String cursor) {
        return route(key).zscan(key, cursor);
    }

    public Long pfadd(String key, String... elements) {
        return route(key).pfadd(key, elements);
    }

    public long pfcount(String key) {
        return route(key).pfcount(key);
    }

    public void flushAll() {
        shardingStrategy.flushAll();
    }
    
}
