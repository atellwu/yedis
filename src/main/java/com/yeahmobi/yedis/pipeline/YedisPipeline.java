package com.yeahmobi.yedis.pipeline;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yeahmobi.yedis.atomic.AtomConfig;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

public class YedisPipeline extends Pipeline {

    private final Jedis         jedis;

    private final Pipeline      pipeline;

    private final AtomicBoolean isReturned = new AtomicBoolean(false);

    private final AtomConfig    config;

    private Pipeline getPipeline() {
        if (!isReturned.get()) {
            return pipeline;
        } else {
            throw new IllegalStateException(
                                            "Pipeline's connection is already close ( or returned to pool ), this pipeline can not do anything.");
        }
    }

    public YedisPipeline(AtomConfig config, Jedis jedis) {
        this.config = config;
        this.jedis = jedis;
        this.pipeline = jedis.pipelined();
    }

    // 可多次调用，第一次正常sync，后续则不做任何事情
    public void sync() {
        if (!isReturned.get()) {
            try {
                pipeline.sync();
            } finally {
                if (isReturned.compareAndSet(false, true)) {
                    jedis.close();
                }
            }
        }
    }

    // 可多次调用，第一次正常返回，后续则返回null
    public List<Object> syncAndReturnAll() {
        if (!isReturned.get()) {
            try {
                List<Object> response = getPipeline().syncAndReturnAll();
                return response;
            } finally {
                if (isReturned.compareAndSet(false, true)) {
                    jedis.close();
                }
            }
        }
        return null;
    }

    // ****以下是代理方法

    public AtomConfig getConfig() {
        return config;
    }

    public Response<List<String>> brpop(String... args) {
        return getPipeline().brpop(args);
    }

    public Response<Long> append(String key, String value) {
        return getPipeline().append(key, value);
    }

    public Response<List<String>> brpop(int timeout, String... keys) {
        return getPipeline().brpop(timeout, keys);
    }

    public Response<Long> append(byte[] key, byte[] value) {
        return getPipeline().append(key, value);
    }

    public Response<List<String>> blpop(String... args) {
        return getPipeline().blpop(args);
    }

    public Response<List<String>> blpop(String key) {
        return getPipeline().blpop(key);
    }

    public Response<List<String>> blpop(int timeout, String... keys) {
        return getPipeline().blpop(timeout, keys);
    }

    public Response<Map<String, String>> blpopMap(int timeout, String... keys) {
        return getPipeline().blpopMap(timeout, keys);
    }

    public Response<List<String>> brpop(String key) {
        return getPipeline().brpop(key);
    }

    public Response<List<byte[]>> brpop(byte[]... args) {
        return getPipeline().brpop(args);
    }

    public Response<List<byte[]>> blpop(byte[] key) {
        return getPipeline().blpop(key);
    }

    public Response<List<String>> brpop(int timeout, byte[]... keys) {
        return getPipeline().brpop(timeout, keys);
    }

    public Response<List<byte[]>> brpop(byte[] key) {
        return getPipeline().brpop(key);
    }

    public Response<Map<String, String>> brpopMap(int timeout, String... keys) {
        return getPipeline().brpopMap(timeout, keys);
    }

    public Response<Long> decr(String key) {
        return getPipeline().decr(key);
    }

    public void setClient(Client client) {
        getPipeline().setClient(client);
    }

    public Response<List<byte[]>> blpop(byte[]... args) {
        return getPipeline().blpop(args);
    }

    public Response<Long> decr(byte[] key) {
        return getPipeline().decr(key);
    }

    public Response<List<String>> blpop(int timeout, byte[]... keys) {
        return getPipeline().blpop(timeout, keys);
    }

    public Response<Long> decrBy(String key, long integer) {
        return getPipeline().decrBy(key, integer);
    }

    public Response<Long> del(String... keys) {
        return getPipeline().del(keys);
    }

    public Response<Long> decrBy(byte[] key, long integer) {
        return getPipeline().decrBy(key, integer);
    }

    public Response<Long> del(byte[]... keys) {
        return getPipeline().del(keys);
    }

    public Response<Long> del(String key) {
        return getPipeline().del(key);
    }

    public Response<Set<String>> keys(String pattern) {
        return getPipeline().keys(pattern);
    }

    public Response<Long> del(byte[] key) {
        return getPipeline().del(key);
    }

    public Response<Set<byte[]>> keys(byte[] pattern) {
        return getPipeline().keys(pattern);
    }

    public Response<String> echo(String string) {
        return getPipeline().echo(string);
    }

    public Response<List<String>> mget(String... keys) {
        return getPipeline().mget(keys);
    }

    public Response<byte[]> echo(byte[] string) {
        return getPipeline().echo(string);
    }

    public Response<List<byte[]>> mget(byte[]... keys) {
        return getPipeline().mget(keys);
    }

    public Response<Boolean> exists(String key) {
        return getPipeline().exists(key);
    }

    public Response<String> mset(String... keysvalues) {
        return getPipeline().mset(keysvalues);
    }

    public Response<Boolean> exists(byte[] key) {
        return getPipeline().exists(key);
    }

    public Response<String> mset(byte[]... keysvalues) {
        return getPipeline().mset(keysvalues);
    }

    public Response<Long> expire(String key, int seconds) {
        return getPipeline().expire(key, seconds);
    }

    public Response<String> discard() {
        return getPipeline().discard();
    }

    public Response<Long> msetnx(String... keysvalues) {
        return getPipeline().msetnx(keysvalues);
    }

    public Response<Long> expire(byte[] key, int seconds) {
        return getPipeline().expire(key, seconds);
    }

    public Response<Long> msetnx(byte[]... keysvalues) {
        return getPipeline().msetnx(keysvalues);
    }

    public Response<List<Object>> exec() {
        return getPipeline().exec();
    }

    public Response<Long> expireAt(String key, long unixTime) {
        return getPipeline().expireAt(key, unixTime);
    }

    public Response<String> rename(String oldkey, String newkey) {
        return getPipeline().rename(oldkey, newkey);
    }

    public Response<Long> expireAt(byte[] key, long unixTime) {
        return getPipeline().expireAt(key, unixTime);
    }

    public Response<String> rename(byte[] oldkey, byte[] newkey) {
        return getPipeline().rename(oldkey, newkey);
    }

    public Response<String> multi() {
        return getPipeline().multi();
    }

    public Response<String> get(String key) {
        return getPipeline().get(key);
    }

    public Response<Long> renamenx(String oldkey, String newkey) {
        return getPipeline().renamenx(oldkey, newkey);
    }

    public Response<byte[]> get(byte[] key) {
        return getPipeline().get(key);
    }

    public boolean equals(Object obj) {
        return getPipeline().equals(obj);
    }

    public Response<Long> renamenx(byte[] oldkey, byte[] newkey) {
        return getPipeline().renamenx(oldkey, newkey);
    }

    public Response<Boolean> getbit(String key, long offset) {
        return getPipeline().getbit(key, offset);
    }

    public Response<String> rpoplpush(String srckey, String dstkey) {
        return getPipeline().rpoplpush(srckey, dstkey);
    }

    public Response<Boolean> getbit(byte[] key, long offset) {
        return getPipeline().getbit(key, offset);
    }

    public Response<byte[]> rpoplpush(byte[] srckey, byte[] dstkey) {
        return getPipeline().rpoplpush(srckey, dstkey);
    }

    public Response<Long> bitpos(String key, boolean value) {
        return getPipeline().bitpos(key, value);
    }

    public Response<Set<String>> sdiff(String... keys) {
        return getPipeline().sdiff(keys);
    }

    public Response<Long> bitpos(String key, boolean value, BitPosParams params) {
        return getPipeline().bitpos(key, value, params);
    }

    public Response<Set<byte[]>> sdiff(byte[]... keys) {
        return getPipeline().sdiff(keys);
    }

    public Response<Long> bitpos(byte[] key, boolean value) {
        return getPipeline().bitpos(key, value);
    }

    public Response<Long> sdiffstore(String dstkey, String... keys) {
        return getPipeline().sdiffstore(dstkey, keys);
    }

    public Response<Long> bitpos(byte[] key, boolean value, BitPosParams params) {
        return getPipeline().bitpos(key, value, params);
    }

    public Response<Long> sdiffstore(byte[] dstkey, byte[]... keys) {
        return getPipeline().sdiffstore(dstkey, keys);
    }

    public Response<String> getrange(String key, long startOffset, long endOffset) {
        return getPipeline().getrange(key, startOffset, endOffset);
    }

    public Response<Set<String>> sinter(String... keys) {
        return getPipeline().sinter(keys);
    }

    public Response<Set<byte[]>> sinter(byte[]... keys) {
        return getPipeline().sinter(keys);
    }

    public Response<String> getSet(String key, String value) {
        return getPipeline().getSet(key, value);
    }

    public Response<Long> sinterstore(String dstkey, String... keys) {
        return getPipeline().sinterstore(dstkey, keys);
    }

    public Response<byte[]> getSet(byte[] key, byte[] value) {
        return getPipeline().getSet(key, value);
    }

    public Response<Long> sinterstore(byte[] dstkey, byte[]... keys) {
        return getPipeline().sinterstore(dstkey, keys);
    }

    public Response<Long> getrange(byte[] key, long startOffset, long endOffset) {
        return getPipeline().getrange(key, startOffset, endOffset);
    }

    public Response<Long> smove(String srckey, String dstkey, String member) {
        return getPipeline().smove(srckey, dstkey, member);
    }

    public Response<Long> hdel(String key, String... field) {
        return getPipeline().hdel(key, field);
    }

    public Response<Long> smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return getPipeline().smove(srckey, dstkey, member);
    }

    public Response<Long> hdel(byte[] key, byte[]... field) {
        return getPipeline().hdel(key, field);
    }

    public Response<Long> sort(String key, SortingParams sortingParameters, String dstkey) {
        return getPipeline().sort(key, sortingParameters, dstkey);
    }

    public Response<Boolean> hexists(String key, String field) {
        return getPipeline().hexists(key, field);
    }

    public Response<Boolean> hexists(byte[] key, byte[] field) {
        return getPipeline().hexists(key, field);
    }

    public Response<Long> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return getPipeline().sort(key, sortingParameters, dstkey);
    }

    public Response<String> hget(String key, String field) {
        return getPipeline().hget(key, field);
    }

    public Response<Long> sort(String key, String dstkey) {
        return getPipeline().sort(key, dstkey);
    }

    public Response<byte[]> hget(byte[] key, byte[] field) {
        return getPipeline().hget(key, field);
    }

    public Response<Long> sort(byte[] key, byte[] dstkey) {
        return getPipeline().sort(key, dstkey);
    }

    public Response<Map<String, String>> hgetAll(String key) {
        return getPipeline().hgetAll(key);
    }

    public Response<Set<String>> sunion(String... keys) {
        return getPipeline().sunion(keys);
    }

    public Response<Map<byte[], byte[]>> hgetAll(byte[] key) {
        return getPipeline().hgetAll(key);
    }

    public Response<Set<byte[]>> sunion(byte[]... keys) {
        return getPipeline().sunion(keys);
    }

    public Response<Long> hincrBy(String key, String field, long value) {
        return getPipeline().hincrBy(key, field, value);
    }

    public Response<Long> sunionstore(String dstkey, String... keys) {
        return getPipeline().sunionstore(dstkey, keys);
    }

    public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
        return getPipeline().hincrBy(key, field, value);
    }

    public Response<Long> sunionstore(byte[] dstkey, byte[]... keys) {
        return getPipeline().sunionstore(dstkey, keys);
    }

    public Response<String> watch(String... keys) {
        return getPipeline().watch(keys);
    }

    public Response<Set<String>> hkeys(String key) {
        return getPipeline().hkeys(key);
    }

    public Response<String> watch(byte[]... keys) {
        return getPipeline().watch(keys);
    }

    public Response<Set<byte[]>> hkeys(byte[] key) {
        return getPipeline().hkeys(key);
    }

    public Response<Long> zinterstore(String dstkey, String... sets) {
        return getPipeline().zinterstore(dstkey, sets);
    }

    public Response<Long> hlen(String key) {
        return getPipeline().hlen(key);
    }

    public Response<Long> zinterstore(byte[] dstkey, byte[]... sets) {
        return getPipeline().zinterstore(dstkey, sets);
    }

    public Response<Long> hlen(byte[] key) {
        return getPipeline().hlen(key);
    }

    public Response<List<String>> hmget(String key, String... fields) {
        return getPipeline().hmget(key, fields);
    }

    public Response<Long> zinterstore(String dstkey, ZParams params, String... sets) {
        return getPipeline().zinterstore(dstkey, params, sets);
    }

    public Response<List<byte[]>> hmget(byte[] key, byte[]... fields) {
        return getPipeline().hmget(key, fields);
    }

    public Response<Long> zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return getPipeline().zinterstore(dstkey, params, sets);
    }

    public Response<String> hmset(String key, Map<String, String> hash) {
        return getPipeline().hmset(key, hash);
    }

    public Response<Long> zunionstore(String dstkey, String... sets) {
        return getPipeline().zunionstore(dstkey, sets);
    }

    public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
        return getPipeline().hmset(key, hash);
    }

    public Response<Long> zunionstore(byte[] dstkey, byte[]... sets) {
        return getPipeline().zunionstore(dstkey, sets);
    }

    public Response<Long> hset(String key, String field, String value) {
        return getPipeline().hset(key, field, value);
    }

    public Response<Long> zunionstore(String dstkey, ZParams params, String... sets) {
        return getPipeline().zunionstore(dstkey, params, sets);
    }

    public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
        return getPipeline().hset(key, field, value);
    }

    public Response<Long> zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return getPipeline().zunionstore(dstkey, params, sets);
    }

    public Response<Long> hsetnx(String key, String field, String value) {
        return getPipeline().hsetnx(key, field, value);
    }

    public Response<String> bgrewriteaof() {
        return getPipeline().bgrewriteaof();
    }

    public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
        return getPipeline().hsetnx(key, field, value);
    }

    public Response<String> bgsave() {
        return getPipeline().bgsave();
    }

    public Response<String> configGet(String pattern) {
        return getPipeline().configGet(pattern);
    }

    public Response<List<String>> hvals(String key) {
        return getPipeline().hvals(key);
    }

    public Response<String> configSet(String parameter, String value) {
        return getPipeline().configSet(parameter, value);
    }

    public Response<List<byte[]>> hvals(byte[] key) {
        return getPipeline().hvals(key);
    }

    public Response<Long> incr(String key) {
        return getPipeline().incr(key);
    }

    public Response<String> brpoplpush(String source, String destination, int timeout) {
        return getPipeline().brpoplpush(source, destination, timeout);
    }

    public Response<Long> incr(byte[] key) {
        return getPipeline().incr(key);
    }

    public Response<byte[]> brpoplpush(byte[] source, byte[] destination, int timeout) {
        return getPipeline().brpoplpush(source, destination, timeout);
    }

    public Response<Long> incrBy(String key, long integer) {
        return getPipeline().incrBy(key, integer);
    }

    public String toString() {
        return getPipeline().toString();
    }

    public Response<Long> incrBy(byte[] key, long integer) {
        return getPipeline().incrBy(key, integer);
    }

    public Response<String> configResetStat() {
        return getPipeline().configResetStat();
    }

    public Response<String> save() {
        return getPipeline().save();
    }

    public Response<String> lindex(String key, long index) {
        return getPipeline().lindex(key, index);
    }

    public Response<Long> lastsave() {
        return getPipeline().lastsave();
    }

    public Response<byte[]> lindex(byte[] key, long index) {
        return getPipeline().lindex(key, index);
    }

    public Response<Long> publish(String channel, String message) {
        return getPipeline().publish(channel, message);
    }

    public Response<Long> linsert(String key, LIST_POSITION where, String pivot, String value) {
        return getPipeline().linsert(key, where, pivot, value);
    }

    public Response<Long> publish(byte[] channel, byte[] message) {
        return getPipeline().publish(channel, message);
    }

    public Response<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return getPipeline().linsert(key, where, pivot, value);
    }

    public Response<String> randomKey() {
        return getPipeline().randomKey();
    }

    public Response<byte[]> randomKeyBinary() {
        return getPipeline().randomKeyBinary();
    }

    public Response<Long> llen(String key) {
        return getPipeline().llen(key);
    }

    public Response<String> flushDB() {
        return getPipeline().flushDB();
    }

    public Response<Long> llen(byte[] key) {
        return getPipeline().llen(key);
    }

    public Response<String> flushAll() {
        return getPipeline().flushAll();
    }

    public Response<String> lpop(String key) {
        return getPipeline().lpop(key);
    }

    public Response<String> info() {
        return getPipeline().info();
    }

    public Response<byte[]> lpop(byte[] key) {
        return getPipeline().lpop(key);
    }

    public Response<List<String>> time() {
        return getPipeline().time();
    }

    public Response<Long> dbSize() {
        return getPipeline().dbSize();
    }

    public Response<Long> lpush(String key, String... string) {
        return getPipeline().lpush(key, string);
    }

    public Response<String> shutdown() {
        return getPipeline().shutdown();
    }

    public Response<Long> lpush(byte[] key, byte[]... string) {
        return getPipeline().lpush(key, string);
    }

    public Response<String> ping() {
        return getPipeline().ping();
    }

    public Response<Long> lpushx(String key, String... string) {
        return getPipeline().lpushx(key, string);
    }

    public Response<String> select(int index) {
        return getPipeline().select(index);
    }

    public Response<Long> bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        return getPipeline().bitop(op, destKey, srcKeys);
    }

    public Response<Long> lpushx(byte[] key, byte[]... bytes) {
        return getPipeline().lpushx(key, bytes);
    }

    public Response<List<String>> lrange(String key, long start, long end) {
        return getPipeline().lrange(key, start, end);
    }

    public Response<Long> bitop(BitOP op, String destKey, String... srcKeys) {
        return getPipeline().bitop(op, destKey, srcKeys);
    }

    public Response<String> clusterNodes() {
        return getPipeline().clusterNodes();
    }

    public Response<List<byte[]>> lrange(byte[] key, long start, long end) {
        return getPipeline().lrange(key, start, end);
    }

    public Response<String> clusterMeet(String ip, int port) {
        return getPipeline().clusterMeet(ip, port);
    }

    public Response<Long> lrem(String key, long count, String value) {
        return getPipeline().lrem(key, count, value);
    }

    public Response<String> clusterAddSlots(int... slots) {
        return getPipeline().clusterAddSlots(slots);
    }

    public Response<Long> lrem(byte[] key, long count, byte[] value) {
        return getPipeline().lrem(key, count, value);
    }

    public Response<String> clusterDelSlots(int... slots) {
        return getPipeline().clusterDelSlots(slots);
    }

    public Response<String> lset(String key, long index, String value) {
        return getPipeline().lset(key, index, value);
    }

    public Response<String> clusterInfo() {
        return getPipeline().clusterInfo();
    }

    public Response<String> lset(byte[] key, long index, byte[] value) {
        return getPipeline().lset(key, index, value);
    }

    public Response<List<String>> clusterGetKeysInSlot(int slot, int count) {
        return getPipeline().clusterGetKeysInSlot(slot, count);
    }

    public Response<String> ltrim(String key, long start, long end) {
        return getPipeline().ltrim(key, start, end);
    }

    public Response<String> clusterSetSlotNode(int slot, String nodeId) {
        return getPipeline().clusterSetSlotNode(slot, nodeId);
    }

    public Response<String> ltrim(byte[] key, long start, long end) {
        return getPipeline().ltrim(key, start, end);
    }

    public Response<String> clusterSetSlotMigrating(int slot, String nodeId) {
        return getPipeline().clusterSetSlotMigrating(slot, nodeId);
    }

    public Response<Long> move(String key, int dbIndex) {
        return getPipeline().move(key, dbIndex);
    }

    public Response<String> clusterSetSlotImporting(int slot, String nodeId) {
        return getPipeline().clusterSetSlotImporting(slot, nodeId);
    }

    public Response<Long> move(byte[] key, int dbIndex) {
        return getPipeline().move(key, dbIndex);
    }

    public Response<Long> persist(String key) {
        return getPipeline().persist(key);
    }

    public Response<String> pfmerge(byte[] destkey, byte[]... sourcekeys) {
        return getPipeline().pfmerge(destkey, sourcekeys);
    }

    public Response<Long> persist(byte[] key) {
        return getPipeline().persist(key);
    }

    public Response<String> pfmerge(String destkey, String... sourcekeys) {
        return getPipeline().pfmerge(destkey, sourcekeys);
    }

    public Response<String> rpop(String key) {
        return getPipeline().rpop(key);
    }

    public Response<Long> pfcount(String... keys) {
        return getPipeline().pfcount(keys);
    }

    public Response<byte[]> rpop(byte[] key) {
        return getPipeline().rpop(key);
    }

    public Response<Long> rpush(String key, String... string) {
        return getPipeline().rpush(key, string);
    }

    public Response<Long> pfcount(byte[]... keys) {
        return getPipeline().pfcount(keys);
    }

    public Response<Long> rpush(byte[] key, byte[]... string) {
        return getPipeline().rpush(key, string);
    }

    public Response<Long> rpushx(String key, String... string) {
        return getPipeline().rpushx(key, string);
    }

    public Response<Long> rpushx(byte[] key, byte[]... string) {
        return getPipeline().rpushx(key, string);
    }

    public Response<Long> sadd(String key, String... member) {
        return getPipeline().sadd(key, member);
    }

    public Response<Long> sadd(byte[] key, byte[]... member) {
        return getPipeline().sadd(key, member);
    }

    public Response<Long> scard(String key) {
        return getPipeline().scard(key);
    }

    public Response<Long> scard(byte[] key) {
        return getPipeline().scard(key);
    }

    public Response<String> set(String key, String value) {
        return getPipeline().set(key, value);
    }

    public Response<String> set(byte[] key, byte[] value) {
        return getPipeline().set(key, value);
    }

    public Response<Boolean> setbit(String key, long offset, boolean value) {
        return getPipeline().setbit(key, offset, value);
    }

    public Response<Boolean> setbit(byte[] key, long offset, byte[] value) {
        return getPipeline().setbit(key, offset, value);
    }

    public Response<String> setex(String key, int seconds, String value) {
        return getPipeline().setex(key, seconds, value);
    }

    public Response<String> setex(byte[] key, int seconds, byte[] value) {
        return getPipeline().setex(key, seconds, value);
    }

    public Response<Long> setnx(String key, String value) {
        return getPipeline().setnx(key, value);
    }

    public Response<Long> setnx(byte[] key, byte[] value) {
        return getPipeline().setnx(key, value);
    }

    public Response<Long> setrange(String key, long offset, String value) {
        return getPipeline().setrange(key, offset, value);
    }

    public Response<Long> setrange(byte[] key, long offset, byte[] value) {
        return getPipeline().setrange(key, offset, value);
    }

    public Response<Boolean> sismember(String key, String member) {
        return getPipeline().sismember(key, member);
    }

    public Response<Boolean> sismember(byte[] key, byte[] member) {
        return getPipeline().sismember(key, member);
    }

    public Response<Set<String>> smembers(String key) {
        return getPipeline().smembers(key);
    }

    public Response<Set<byte[]>> smembers(byte[] key) {
        return getPipeline().smembers(key);
    }

    public Response<List<String>> sort(String key) {
        return getPipeline().sort(key);
    }

    public Response<List<byte[]>> sort(byte[] key) {
        return getPipeline().sort(key);
    }

    public Response<List<String>> sort(String key, SortingParams sortingParameters) {
        return getPipeline().sort(key, sortingParameters);
    }

    public Response<List<byte[]>> sort(byte[] key, SortingParams sortingParameters) {
        return getPipeline().sort(key, sortingParameters);
    }

    public Response<String> spop(String key) {
        return getPipeline().spop(key);
    }

    public Response<byte[]> spop(byte[] key) {
        return getPipeline().spop(key);
    }

    public Response<String> srandmember(String key) {
        return getPipeline().srandmember(key);
    }

    public Response<List<String>> srandmember(String key, int count) {
        return getPipeline().srandmember(key, count);
    }

    public Response<byte[]> srandmember(byte[] key) {
        return getPipeline().srandmember(key);
    }

    public Response<List<byte[]>> srandmember(byte[] key, int count) {
        return getPipeline().srandmember(key, count);
    }

    public Response<Long> srem(String key, String... member) {
        return getPipeline().srem(key, member);
    }

    public Response<Long> srem(byte[] key, byte[]... member) {
        return getPipeline().srem(key, member);
    }

    public Response<Long> strlen(String key) {
        return getPipeline().strlen(key);
    }

    public Response<Long> strlen(byte[] key) {
        return getPipeline().strlen(key);
    }

    public Response<String> substr(String key, int start, int end) {
        return getPipeline().substr(key, start, end);
    }

    public Response<String> substr(byte[] key, int start, int end) {
        return getPipeline().substr(key, start, end);
    }

    public Response<Long> ttl(String key) {
        return getPipeline().ttl(key);
    }

    public Response<Long> ttl(byte[] key) {
        return getPipeline().ttl(key);
    }

    public Response<String> type(String key) {
        return getPipeline().type(key);
    }

    public Response<String> type(byte[] key) {
        return getPipeline().type(key);
    }

    public Response<Long> zadd(String key, double score, String member) {
        return getPipeline().zadd(key, score, member);
    }

    public Response<Long> zadd(String key, Map<String, Double> scoreMembers) {
        return getPipeline().zadd(key, scoreMembers);
    }

    public Response<Long> zadd(byte[] key, double score, byte[] member) {
        return getPipeline().zadd(key, score, member);
    }

    public Response<Long> zcard(String key) {
        return getPipeline().zcard(key);
    }

    public Response<Long> zcard(byte[] key) {
        return getPipeline().zcard(key);
    }

    public Response<Long> zcount(String key, double min, double max) {
        return getPipeline().zcount(key, min, max);
    }

    public Response<Long> zcount(String key, String min, String max) {
        return getPipeline().zcount(key, min, max);
    }

    public Response<Long> zcount(byte[] key, double min, double max) {
        return getPipeline().zcount(key, min, max);
    }

    public Response<Double> zincrby(String key, double score, String member) {
        return getPipeline().zincrby(key, score, member);
    }

    public Response<Double> zincrby(byte[] key, double score, byte[] member) {
        return getPipeline().zincrby(key, score, member);
    }

    public Response<Set<String>> zrange(String key, long start, long end) {
        return getPipeline().zrange(key, start, end);
    }

    public Response<Set<byte[]>> zrange(byte[] key, long start, long end) {
        return getPipeline().zrange(key, start, end);
    }

    public Response<Set<String>> zrangeByScore(String key, double min, double max) {
        return getPipeline().zrangeByScore(key, min, max);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max) {
        return getPipeline().zrangeByScore(key, min, max);
    }

    public Response<Set<String>> zrangeByScore(String key, String min, String max) {
        return getPipeline().zrangeByScore(key, min, max);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return getPipeline().zrangeByScore(key, min, max);
    }

    public Response<Set<String>> zrangeByScore(String key, double min, double max, int offset, int count) {
        return getPipeline().zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<String>> zrangeByScore(String key, String min, String max, int offset, int count) {
        return getPipeline().zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return getPipeline().zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return getPipeline().zrangeByScore(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max) {
        return getPipeline().zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max) {
        return getPipeline().zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return getPipeline().zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return getPipeline().zrangeByScoreWithScores(key, min, max);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return getPipeline().zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return getPipeline().zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return getPipeline().zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return getPipeline().zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Response<Set<String>> zrevrangeByScore(String key, double max, double min) {
        return getPipeline().zrevrangeByScore(key, max, min);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min) {
        return getPipeline().zrevrangeByScore(key, max, min);
    }

    public Response<Set<String>> zrevrangeByScore(String key, String max, String min) {
        return getPipeline().zrevrangeByScore(key, max, min);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return getPipeline().zrevrangeByScore(key, max, min);
    }

    public Response<Set<String>> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return getPipeline().zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<String>> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return getPipeline().zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return getPipeline().zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return getPipeline().zrevrangeByScore(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return getPipeline().zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Response<Set<Tuple>> zrangeWithScores(String key, long start, long end) {
        return getPipeline().zrangeWithScores(key, start, end);
    }

    public Response<Set<Tuple>> zrangeWithScores(byte[] key, long start, long end) {
        return getPipeline().zrangeWithScores(key, start, end);
    }

    public Response<Long> zrank(String key, String member) {
        return getPipeline().zrank(key, member);
    }

    public Response<Long> zrank(byte[] key, byte[] member) {
        return getPipeline().zrank(key, member);
    }

    public Response<Long> zrem(String key, String... member) {
        return getPipeline().zrem(key, member);
    }

    public Response<Long> zrem(byte[] key, byte[]... member) {
        return getPipeline().zrem(key, member);
    }

    public Response<Long> zremrangeByRank(String key, long start, long end) {
        return getPipeline().zremrangeByRank(key, start, end);
    }

    public Response<Long> zremrangeByRank(byte[] key, long start, long end) {
        return getPipeline().zremrangeByRank(key, start, end);
    }

    public Response<Long> zremrangeByScore(String key, double start, double end) {
        return getPipeline().zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(String key, String start, String end) {
        return getPipeline().zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
        return getPipeline().zremrangeByScore(key, start, end);
    }

    public Response<Long> zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return getPipeline().zremrangeByScore(key, start, end);
    }

    public Response<Set<String>> zrevrange(String key, long start, long end) {
        return getPipeline().zrevrange(key, start, end);
    }

    public Response<Set<byte[]>> zrevrange(byte[] key, long start, long end) {
        return getPipeline().zrevrange(key, start, end);
    }

    public Response<Set<Tuple>> zrevrangeWithScores(String key, long start, long end) {
        return getPipeline().zrevrangeWithScores(key, start, end);
    }

    public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, long start, long end) {
        return getPipeline().zrevrangeWithScores(key, start, end);
    }

    public Response<Long> zrevrank(String key, String member) {
        return getPipeline().zrevrank(key, member);
    }

    public Response<Long> zrevrank(byte[] key, byte[] member) {
        return getPipeline().zrevrank(key, member);
    }

    public Response<Double> zscore(String key, String member) {
        return getPipeline().zscore(key, member);
    }

    public Response<Double> zscore(byte[] key, byte[] member) {
        return getPipeline().zscore(key, member);
    }

    public Response<Long> bitcount(String key) {
        return getPipeline().bitcount(key);
    }

    public Response<Long> bitcount(String key, long start, long end) {
        return getPipeline().bitcount(key, start, end);
    }

    public Response<Long> bitcount(byte[] key) {
        return getPipeline().bitcount(key);
    }

    public Response<Long> bitcount(byte[] key, long start, long end) {
        return getPipeline().bitcount(key, start, end);
    }

    public Response<byte[]> dump(String key) {
        return getPipeline().dump(key);
    }

    public Response<byte[]> dump(byte[] key) {
        return getPipeline().dump(key);
    }

    public Response<String> migrate(String host, int port, String key, int destinationDb, int timeout) {
        return getPipeline().migrate(host, port, key, destinationDb, timeout);
    }

    public Response<String> migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
        return getPipeline().migrate(host, port, key, destinationDb, timeout);
    }

    public Response<Long> objectRefcount(String key) {
        return getPipeline().objectRefcount(key);
    }

    public Response<Long> objectRefcount(byte[] key) {
        return getPipeline().objectRefcount(key);
    }

    public Response<String> objectEncoding(String key) {
        return getPipeline().objectEncoding(key);
    }

    public Response<byte[]> objectEncoding(byte[] key) {
        return getPipeline().objectEncoding(key);
    }

    public Response<Long> objectIdletime(String key) {
        return getPipeline().objectIdletime(key);
    }

    public Response<Long> objectIdletime(byte[] key) {
        return getPipeline().objectIdletime(key);
    }

    @Deprecated
    public Response<Long> pexpire(String key, int milliseconds) {
        return getPipeline().pexpire(key, milliseconds);
    }

    @Deprecated
    public Response<Long> pexpire(byte[] key, int milliseconds) {
        return getPipeline().pexpire(key, milliseconds);
    }

    public Response<Long> pexpire(String key, long milliseconds) {
        return getPipeline().pexpire(key, milliseconds);
    }

    public Response<Long> pexpire(byte[] key, long milliseconds) {
        return getPipeline().pexpire(key, milliseconds);
    }

    public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
        return getPipeline().pexpireAt(key, millisecondsTimestamp);
    }

    public Response<Long> pexpireAt(byte[] key, long millisecondsTimestamp) {
        return getPipeline().pexpireAt(key, millisecondsTimestamp);
    }

    public Response<Long> pttl(String key) {
        return getPipeline().pttl(key);
    }

    public Response<Long> pttl(byte[] key) {
        return getPipeline().pttl(key);
    }

    public Response<String> restore(String key, int ttl, byte[] serializedValue) {
        return getPipeline().restore(key, ttl, serializedValue);
    }

    public Response<String> restore(byte[] key, int ttl, byte[] serializedValue) {
        return getPipeline().restore(key, ttl, serializedValue);
    }

    public Response<Double> incrByFloat(String key, double increment) {
        return getPipeline().incrByFloat(key, increment);
    }

    public Response<Double> incrByFloat(byte[] key, double increment) {
        return getPipeline().incrByFloat(key, increment);
    }

    public Response<String> psetex(String key, int milliseconds, String value) {
        return getPipeline().psetex(key, milliseconds, value);
    }

    public Response<String> psetex(byte[] key, int milliseconds, byte[] value) {
        return getPipeline().psetex(key, milliseconds, value);
    }

    public Response<String> set(String key, String value, String nxxx) {
        return getPipeline().set(key, value, nxxx);
    }

    public Response<String> set(byte[] key, byte[] value, byte[] nxxx) {
        return getPipeline().set(key, value, nxxx);
    }

    public Response<String> set(String key, String value, String nxxx, String expx, int time) {
        return getPipeline().set(key, value, nxxx, expx, time);
    }

    public Response<String> set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
        return getPipeline().set(key, value, nxxx, expx, time);
    }

    public Response<Double> hincrByFloat(String key, String field, double increment) {
        return getPipeline().hincrByFloat(key, field, increment);
    }

    public Response<Double> hincrByFloat(byte[] key, byte[] field, double increment) {
        return getPipeline().hincrByFloat(key, field, increment);
    }

    public Response<String> eval(String script) {
        return getPipeline().eval(script);
    }

    public Response<String> eval(String script, List<String> keys, List<String> args) {
        return getPipeline().eval(script, keys, args);
    }

    public Response<String> eval(String script, int numKeys, String[] argv) {
        return getPipeline().eval(script, numKeys, argv);
    }

    public Response<String> evalsha(String script) {
        return getPipeline().evalsha(script);
    }

    public Response<String> evalsha(String sha1, List<String> keys, List<String> args) {
        return getPipeline().evalsha(sha1, keys, args);
    }

    public Response<String> evalsha(String sha1, int numKeys, String[] argv) {
        return getPipeline().evalsha(sha1, numKeys, argv);
    }

    public Response<Long> pfadd(byte[] key, byte[]... elements) {
        return getPipeline().pfadd(key, elements);
    }

    public Response<Long> pfcount(byte[] key) {
        return getPipeline().pfcount(key);
    }

    public Response<Long> pfadd(String key, String... elements) {
        return getPipeline().pfadd(key, elements);
    }

    public Response<Long> pfcount(String key) {
        return getPipeline().pfcount(key);
    }

}
