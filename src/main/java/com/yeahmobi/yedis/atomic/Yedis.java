package com.yeahmobi.yedis.atomic;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Slowlog;

import com.yeahmobi.yedis.async.AsyncOperation;
import com.yeahmobi.yedis.async.JedisPoolExecutor;
import com.yeahmobi.yedis.async.OperationResult;
import com.yeahmobi.yedis.common.YedisException;
import com.yeahmobi.yedis.common.YedisNetworkException;
import com.yeahmobi.yedis.common.YedisTimeoutException;

/**
 * 最基础的redis客户端类，包含连接池的功能
 * 
 * @author atell
 */
public final class Yedis {

    private static final TimeUnit   UNIT     = TimeUnit.MILLISECONDS;

    private final long              timeout;

    private final JedisPoolExecutor executor;

    private AtomConfig              config;

    private AtomicBoolean           shutdown = new AtomicBoolean(false);

    public AtomConfig getConfig() {
        return config;
    }

    public void setConfig(AtomConfig config) {
        this.config = config;
    }

    public Yedis(AtomConfig config) {
        this.config = config;

        timeout = config.getTimeout();

        // 构建executor
        executor = new JedisPoolExecutor(config);
        executor.start();
    }

    public void close() {
        if (shutdown.compareAndSet(false, true)) {
            executor.shutdown();
        }
    }

    private <T> T doAsynchronously(AsyncOperation<T> opr) {
        Future<OperationResult<T>> future = executor.submit(opr);

        try {
            OperationResult<T> result = future.get(timeout, UNIT);
            return result.getValue();
        } catch (InterruptedException e) {
            future.cancel(true);
            throw new YedisException(e.getMessage(), e);
        } catch (ExecutionException e) {
            future.cancel(true);
            if (e.getCause() instanceof JedisConnectionException) {
                throw new YedisNetworkException(e.getMessage(), e.getCause());
            } else {
                throw new YedisException(e.getMessage(), e.getCause());
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new YedisTimeoutException(e.getMessage(), e);
        } catch (CancellationException e) {
            // Operation is cancelled by JedisPoolExecutor, maybe caused by network problem or Yedis is closing.
            throw new YedisException(
                                     "Operation is cancelled by JedisPoolExecutor, maybe caused by network problem or Yedis is closing.",
                                     e);
        } catch (RuntimeException e) {
            future.cancel(true);
            throw new YedisException(e.getMessage(), e);
        }
    }

    public String set(final String key, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value));
            }
        });
    }

    public String set(final String key, final String value, final String nxxx, final String expx, final long time) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx, expx, time));
            }
        });
    }

    public String get(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.get(key));
            }
        });
    }

    public Boolean exists(final String key) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.exists(key));
            }
        });
    }

    public Long del(final String... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.del(keys));
            }
        });
    }

    public Long del(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.del(key));
            }
        });
    }

    public String type(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.type(key));
            }
        });
    }

    public Set<String> keys(final String pattern) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.keys(pattern));
            }
        });
    }

    public String randomKey() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.randomKey());
            }
        });
    }

    public String rename(final String oldkey, final String newkey) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.rename(oldkey, newkey));
            }
        });
    }

    public Long renamenx(final String oldkey, final String newkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.renamenx(oldkey, newkey));
            }
        });
    }

    public Long expire(final String key, final int seconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.expire(key, seconds));
            }
        });
    }

    public Long expireAt(final String key, final long unixTime) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.expireAt(key, unixTime));
            }
        });
    }

    public Long ttl(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.ttl(key));
            }
        });
    }

    public Long move(final String key, final int dbIndex) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.move(key, dbIndex));
            }
        });
    }

    public String getSet(final String key, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.getSet(key, value));
            }
        });
    }

    public List<String> mget(final String... keys) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.mget(keys));
            }
        });
    }

    public Long setnx(final String key, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.setnx(key, value));
            }
        });
    }

    public String setex(final String key, final int seconds, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.setex(key, seconds, value));
            }
        });
    }

    public String mset(final String... keysvalues) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.mset(keysvalues));
            }
        });
    }

    public Long msetnx(final String... keysvalues) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.msetnx(keysvalues));
            }
        });
    }

    public Long decrBy(final String key, final long integer) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.decrBy(key, integer));
            }
        });
    }

    public Long decr(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.decr(key));
            }
        });
    }

    public Long incrBy(final String key, final long integer) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.incrBy(key, integer));
            }
        });
    }

    public Double incrByFloat(final String key, final double value) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.incrByFloat(key, value));
            }
        });
    }

    public Long incr(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.incr(key));
            }
        });
    }

    public Long append(final String key, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.append(key, value));
            }
        });
    }

    public String substr(final String key, final int start, final int end) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.substr(key, start, end));
            }
        });
    }

    public Long hset(final String key, final String field, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hset(key, field, value));
            }
        });
    }

    public String hget(final String key, final String field) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.hget(key, field));
            }
        });
    }

    public Long hsetnx(final String key, final String field, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hsetnx(key, field, value));
            }
        });
    }

    public String hmset(final String key, final Map<String, String> hash) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.hmset(key, hash));
            }
        });
    }

    public List<String> hmget(final String key, final String... fields) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.hmget(key, fields));
            }
        });
    }

    public Long hincrBy(final String key, final String field, final long value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hincrBy(key, field, value));
            }
        });
    }

    public Double hincrByFloat(final String key, final String field, final double value) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.hincrByFloat(key, field, value));
            }
        });
    }

    public Boolean hexists(final String key, final String field) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.hexists(key, field));
            }
        });
    }

    public Long hdel(final String key, final String... fields) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hdel(key, fields));
            }
        });
    }

    public Long hlen(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hlen(key));
            }
        });
    }

    public Set<String> hkeys(final String key) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.hkeys(key));
            }
        });
    }

    public List<String> hvals(final String key) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.hvals(key));
            }
        });
    }

    public Map<String, String> hgetAll(final String key) {
        return doAsynchronously(new AsyncOperation<Map<String, String>>() {

            @Override
            public OperationResult<Map<String, String>> execute(Jedis jedis) {
                return new OperationResult<Map<String, String>>(jedis.hgetAll(key));
            }
        });
    }

    public Long rpush(final String key, final String... strings) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.rpush(key, strings));
            }
        });
    }

    public Long lpush(final String key, final String... strings) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lpush(key, strings));
            }
        });
    }

    public Long llen(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.llen(key));
            }
        });
    }

    public List<String> lrange(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.lrange(key, start, end));
            }
        });
    }

    public String ltrim(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.ltrim(key, start, end));
            }
        });
    }

    public String lindex(final String key, final long index) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.lindex(key, index));
            }
        });
    }

    public String lset(final String key, final long index, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.lset(key, index, value));
            }
        });
    }

    public Long lrem(final String key, final long count, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lrem(key, count, value));
            }
        });
    }

    public String lpop(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.lpop(key));
            }
        });
    }

    public String rpop(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.rpop(key));
            }
        });
    }

    public String rpoplpush(final String srckey, final String dstkey) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.rpoplpush(srckey, dstkey));
            }
        });
    }

    public Long sadd(final String key, final String... members) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sadd(key, members));
            }
        });
    }

    public Set<String> smembers(final String key) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.smembers(key));
            }
        });
    }

    public Long srem(final String key, final String... members) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.srem(key, members));
            }
        });
    }

    public String spop(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.spop(key));
            }
        });
    }

    public Long smove(final String srckey, final String dstkey, final String member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.smove(srckey, dstkey, member));
            }
        });
    }

    public Long scard(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.scard(key));
            }
        });
    }

    public Boolean sismember(final String key, final String member) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.sismember(key, member));
            }
        });
    }

    public Set<String> sinter(final String... keys) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.sinter(keys));
            }
        });
    }

    public Long sinterstore(final String dstkey, final String... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sinterstore(dstkey, keys));
            }
        });
    }

    public Set<String> sunion(final String... keys) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.sunion(keys));
            }
        });
    }

    public Long sunionstore(final String dstkey, final String... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sunionstore(dstkey, keys));
            }
        });
    }

    public Set<String> sdiff(final String... keys) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.sdiff(keys));
            }
        });
    }

    public Long sdiffstore(final String dstkey, final String... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sdiffstore(dstkey, keys));
            }
        });
    }

    public String srandmember(final String key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.srandmember(key));
            }
        });
    }

    public List<String> srandmember(final String key, final int count) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.srandmember(key, count));
            }
        });
    }

    public Long zadd(final String key, final double score, final String member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zadd(key, score, member));
            }
        });
    }

    public Long zadd(final String key, final Map<String, Double> scoreMembers) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zadd(key, scoreMembers));
            }
        });
    }

    public Set<String> zrange(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrange(key, start, end));
            }
        });
    }

    public Long zrem(final String key, final String... members) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrem(key, members));
            }
        });
    }

    public Double zincrby(final String key, final double score, final String member) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.zincrby(key, score, member));
            }
        });
    }

    public Long zrank(final String key, final String member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrank(key, member));
            }
        });
    }

    public Long zrevrank(final String key, final String member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrevrank(key, member));
            }
        });
    }

    public Set<String> zrevrange(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrevrange(key, start, end));
            }
        });
    }

    public Set<Tuple> zrangeWithScores(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeWithScores(key, start, end));
            }
        });
    }

    public Set<Tuple> zrevrangeWithScores(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeWithScores(key, start, end));
            }
        });
    }

    public Long zcard(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcard(key));
            }
        });
    }

    public Double zscore(final String key, final String member) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.zscore(key, member));
            }
        });
    }

    public List<String> sort(final String key) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.sort(key));
            }
        });
    }

    public List<String> sort(final String key, final SortingParams sortingParameters) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.sort(key, sortingParameters));
            }
        });
    }

    public List<String> blpop(final int timeout, final String... keys) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.blpop(timeout, keys));
            }
        });
    }

    public Long sort(final String key, final SortingParams sortingParameters, final String dstkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sort(key, sortingParameters, dstkey));
            }
        });
    }

    public Long sort(final String key, final String dstkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sort(key, dstkey));
            }
        });
    }

    public List<String> brpop(final int timeout, final String... keys) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.brpop(timeout, keys));
            }
        });
    }

    public Long zcount(final String key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcount(key, min, max));
            }
        });
    }

    public Long zcount(final String key, final String min, final String max) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcount(key, min, max));
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrangeByScore(key, min, max));
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final String min, final String max) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrangeByScore(key, min, max));
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final double min, final double max, final int offset,
                                     final int count) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrangeByScore(key, min, max, offset, count));
            }
        });
    }

    public Set<String> zrangeByScore(final String key, final String min, final String max, final int offset,
                                     final int count) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrangeByScore(key, min, max, offset, count));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset,
                                              final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final String key, final String min, final String max, final int offset,
                                              final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrevrangeByScore(key, max, min));
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final String max, final String min) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrevrangeByScore(key, max, min));
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset,
                                        final int count) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrevrangeByScore(key, max, min, offset, count));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final double max, final double min,
                                                 final int offset, final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min,
                                                 final int offset, final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
            }
        });
    }

    public Set<String> zrevrangeByScore(final String key, final String max, final String min, final int offset,
                                        final int count) {
        return doAsynchronously(new AsyncOperation<Set<String>>() {

            @Override
            public OperationResult<Set<String>> execute(Jedis jedis) {
                return new OperationResult<Set<String>>(jedis.zrevrangeByScore(key, max, min, offset, count));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final String key, final String max, final String min) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min));
            }
        });
    }

    public Long zremrangeByRank(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByRank(key, start, end));
            }
        });
    }

    public Long zremrangeByScore(final String key, final double start, final double end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByScore(key, start, end));
            }
        });
    }

    public Long zremrangeByScore(final String key, final String start, final String end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByScore(key, start, end));
            }
        });
    }

    public Long zunionstore(final String dstkey, final String... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zunionstore(dstkey, sets));
            }
        });
    }

    public Long zunionstore(final String dstkey, final ZParams params, final String... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zunionstore(dstkey, params, sets));
            }
        });
    }

    public Long zinterstore(final String dstkey, final String... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zinterstore(dstkey, sets));
            }
        });
    }

    public Long zinterstore(final String dstkey, final ZParams params, final String... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zinterstore(dstkey, params, sets));
            }
        });
    }

    public Long strlen(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.strlen(key));
            }
        });
    }

    public Long lpushx(final String key, final String... string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lpushx(key, string));
            }
        });
    }

    public Long lpushx(final byte[] key, final byte[]... string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lpushx(key, string));
            }
        });
    }

    public Long persist(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.persist(key));
            }
        });
    }

    public Long rpushx(final String key, final String... string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.rpushx(key, string));
            }
        });
    }

    public String echo(final String string) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.echo(string));
            }
        });
    }

    public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.linsert(key, where, pivot, value));
            }
        });
    }

    public String brpoplpush(final String source, final String destination, final int timeout) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.brpoplpush(source, destination, timeout));
            }
        });
    }

    public Boolean setbit(final String key, final long offset, final boolean value) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.setbit(key, offset, value));
            }
        });
    }

    public Boolean setbit(final String key, final long offset, final String value) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.setbit(key, offset, value));
            }
        });
    }

    public Boolean getbit(final String key, final long offset) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.getbit(key, offset));
            }
        });
    }

    public Long setrange(final String key, final long offset, final String value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.setrange(key, offset, value));
            }
        });
    }

    public String getrange(final String key, final long startOffset, final long endOffset) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.getrange(key, startOffset, endOffset));
            }
        });
    }

    public Long bitpos(final String key, final boolean value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitpos(key, value));
            }
        });
    }

    public Long bitpos(final String key, final boolean value, final BitPosParams params) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitpos(key, value, params));
            }
        });
    }

    public List<String> configGet(final String pattern) {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.configGet(pattern));
            }
        });
    }

    public String configSet(final String parameter, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.configSet(parameter, value));
            }
        });
    }

    public Object eval(final String script, final int keyCount, final String... params) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script, keyCount, params));
            }
        });
    }

    public Object eval(final String script, final List<String> keys, final List<String> args) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script, keys, args));
            }
        });
    }

    public Object eval(final String script) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script));
            }
        });
    }

    public Object evalsha(final String script) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(script));
            }
        });
    }

    public Object evalsha(final String sha1, final List<String> keys, final List<String> args) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(sha1, keys, args));
            }
        });
    }

    public Object evalsha(final String sha1, final int keyCount, final String... params) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(sha1, keyCount, params));
            }
        });
    }

    public Boolean scriptExists(final String sha1) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.scriptExists(sha1));
            }
        });
    }

    public List<Boolean> scriptExists(final String... sha1) {
        return doAsynchronously(new AsyncOperation<List<Boolean>>() {

            @Override
            public OperationResult<List<Boolean>> execute(Jedis jedis) {
                return new OperationResult<List<Boolean>>(jedis.scriptExists(sha1));
            }
        });
    }

    public String scriptLoad(final String script) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.scriptLoad(script));
            }
        });
    }

    public List<Slowlog> slowlogGet() {
        return doAsynchronously(new AsyncOperation<List<Slowlog>>() {

            @Override
            public OperationResult<List<Slowlog>> execute(Jedis jedis) {
                return new OperationResult<List<Slowlog>>(jedis.slowlogGet());
            }
        });
    }

    public List<Slowlog> slowlogGet(final long entries) {
        return doAsynchronously(new AsyncOperation<List<Slowlog>>() {

            @Override
            public OperationResult<List<Slowlog>> execute(Jedis jedis) {
                return new OperationResult<List<Slowlog>>(jedis.slowlogGet(entries));
            }
        });
    }

    public Long objectRefcount(final String string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.objectRefcount(string));
            }
        });
    }

    public String objectEncoding(final String string) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.objectEncoding(string));
            }
        });
    }

    public Long objectIdletime(final String string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.objectIdletime(string));
            }
        });
    }

    public Long bitcount(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitcount(key));
            }
        });
    }

    public Long bitcount(final String key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitcount(key, start, end));
            }
        });
    }

    public Long bitop(final BitOP op, final String destKey, final String... srcKeys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitop(op, destKey, srcKeys));
            }
        });
    }

    public byte[] dump(final String key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.dump(key));
            }
        });
    }

    public String restore(final String key, final int ttl, final byte[] serializedValue) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.restore(key, ttl, serializedValue));
            }
        });
    }

    @Deprecated
    public Long pexpire(final String key, final int milliseconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpire(key, milliseconds));
            }
        });
    }

    public Long pexpire(final String key, final long milliseconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpire(key, milliseconds));
            }
        });
    }

    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpireAt(key, millisecondsTimestamp));
            }
        });
    }

    public Long pttl(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pttl(key));
            }
        });
    }

    public String psetex(final String key, final int milliseconds, final String value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.psetex(key, milliseconds, value));
            }
        });
    }

    public String set(final String key, final String value, final String nxxx) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx));
            }
        });
    }

    public String set(final String key, final String value, final String nxxx, final String expx, final int time) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx, expx, time));
            }
        });
    }

    public String clientKill(final String client) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientKill(client));
            }
        });
    }

    public String clientSetname(final String name) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientSetname(name));
            }
        });
    }

    public String migrate(final String host, final int port, final String key, final int destinationDb,
                          final int timeout) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.migrate(host, port, key, destinationDb, timeout));
            }
        });
    }

    @Deprecated
    public ScanResult<String> scan(final int cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.scan(cursor));
            }
        });
    }

    @Deprecated
    public ScanResult<String> scan(final int cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.scan(cursor, params));
            }
        });
    }

    @Deprecated
    public ScanResult<Entry<String, String>> hscan(final String key, final int cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<String, String>>>() {

            @Override
            public OperationResult<ScanResult<Entry<String, String>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<String, String>>>(jedis.hscan(key, cursor));
            }
        });
    }

    @Deprecated
    public ScanResult<Entry<String, String>> hscan(final String key, final int cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<String, String>>>() {

            @Override
            public OperationResult<ScanResult<Entry<String, String>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<String, String>>>(jedis.hscan(key, cursor, params));
            }
        });
    }

    @Deprecated
    public ScanResult<String> sscan(final String key, final int cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.sscan(key, cursor));
            }
        });
    }

    @Deprecated
    public ScanResult<String> sscan(final String key, final int cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.sscan(key, cursor, params));
            }
        });
    }

    @Deprecated
    public ScanResult<Tuple> zscan(final String key, final int cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor));
            }
        });
    }

    @Deprecated
    public ScanResult<Tuple> zscan(final String key, final int cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor, params));
            }
        });
    }

    public ScanResult<String> scan(final String cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.scan(cursor));
            }
        });
    }

    public ScanResult<String> scan(final String cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.scan(cursor, params));
            }
        });
    }

    public ScanResult<Entry<String, String>> hscan(final String key, final String cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<String, String>>>() {

            @Override
            public OperationResult<ScanResult<Entry<String, String>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<String, String>>>(jedis.hscan(key, cursor));
            }
        });
    }

    public ScanResult<Entry<String, String>> hscan(final String key, final String cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<String, String>>>() {

            @Override
            public OperationResult<ScanResult<Entry<String, String>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<String, String>>>(jedis.hscan(key, cursor, params));
            }
        });
    }

    public ScanResult<String> sscan(final String key, final String cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.sscan(key, cursor));
            }
        });
    }

    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<String>>() {

            @Override
            public OperationResult<ScanResult<String>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<String>>(jedis.sscan(key, cursor, params));
            }
        });
    }

    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor));
            }
        });
    }

    public ScanResult<Tuple> zscan(final String key, final String cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor, params));
            }
        });
    }

    public long pfcount(final String key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfcount(key));
            }
        });
    }

    public Long pfcount(final String... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfcount(keys));
            }
        });
    }

    public String pfmerge(final String destkey, final String... sourcekeys) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.pfmerge(destkey, sourcekeys));
            }
        });
    }

    public String ping() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.ping());
            }
        });
    }

    public String set(final byte[] key, final byte[] value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value));
            }
        });
    }

    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx, expx, time));
            }
        });
    }

    public byte[] get(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.get(key));
            }
        });
    }

    public String quit() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.quit());
            }
        });
    }

    public Boolean exists(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.exists(key));
            }
        });
    }

    public Long del(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.del(keys));
            }
        });
    }

    public Long del(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.del(key));
            }
        });
    }

    public String type(final byte[] key) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.type(key));
            }
        });
    }

    public String flushDB() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.flushDB());
            }
        });
    }

    public Set<byte[]> keys(final byte[] pattern) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.keys(pattern));
            }
        });
    }

    public byte[] randomBinaryKey() {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.randomBinaryKey());
            }
        });
    }

    public String rename(final byte[] oldkey, final byte[] newkey) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.rename(oldkey, newkey));
            }
        });
    }

    public Long renamenx(final byte[] oldkey, final byte[] newkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.renamenx(oldkey, newkey));
            }
        });
    }

    public Long dbSize() {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.dbSize());
            }
        });
    }

    public Long expire(final byte[] key, final int seconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.expire(key, seconds));
            }
        });
    }

    public Long expireAt(final byte[] key, final long unixTime) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.expireAt(key, unixTime));
            }
        });
    }

    public Long ttl(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.ttl(key));
            }
        });
    }

    public String select(final int index) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.select(index));
            }
        });
    }

    public Long move(final byte[] key, final int dbIndex) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.move(key, dbIndex));
            }
        });
    }

    public String flushAll() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.flushAll());
            }
        });
    }

    public byte[] getSet(final byte[] key, final byte[] value) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.getSet(key, value));
            }
        });
    }

    public List<byte[]> mget(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.mget(keys));
            }
        });
    }

    public Long setnx(final byte[] key, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.setnx(key, value));
            }
        });
    }

    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.setex(key, seconds, value));
            }
        });
    }

    public String mset(final byte[]... keysvalues) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.mset(keysvalues));
            }
        });
    }

    public Long msetnx(final byte[]... keysvalues) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.msetnx(keysvalues));
            }
        });
    }

    public Long decrBy(final byte[] key, final long integer) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.decrBy(key, integer));
            }
        });
    }

    public Long decr(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.decr(key));
            }
        });
    }

    public Long incrBy(final byte[] key, final long integer) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.incrBy(key, integer));
            }
        });
    }

    public Double incrByFloat(final byte[] key, final double integer) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.incrByFloat(key, integer));
            }
        });
    }

    public Long incr(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.incr(key));
            }
        });
    }

    public Long append(final byte[] key, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.append(key, value));
            }
        });
    }

    public byte[] substr(final byte[] key, final int start, final int end) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.substr(key, start, end));
            }
        });
    }

    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hset(key, field, value));
            }
        });
    }

    public byte[] hget(final byte[] key, final byte[] field) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.hget(key, field));
            }
        });
    }

    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hsetnx(key, field, value));
            }
        });
    }

    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.hmset(key, hash));
            }
        });
    }

    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.hmget(key, fields));
            }
        });
    }

    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hincrBy(key, field, value));
            }
        });
    }

    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.hincrByFloat(key, field, value));
            }
        });
    }

    public Boolean hexists(final byte[] key, final byte[] field) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.hexists(key, field));
            }
        });
    }

    public Long hdel(final byte[] key, final byte[]... fields) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hdel(key, fields));
            }
        });
    }

    public Long hlen(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.hlen(key));
            }
        });
    }

    public Set<byte[]> hkeys(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.hkeys(key));
            }
        });
    }

    public List<byte[]> hvals(final byte[] key) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.hvals(key));
            }
        });
    }

    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Map<byte[], byte[]>>() {

            @Override
            public OperationResult<Map<byte[], byte[]>> execute(Jedis jedis) {
                return new OperationResult<Map<byte[], byte[]>>(jedis.hgetAll(key));
            }
        });
    }

    public Long rpush(final byte[] key, final byte[]... strings) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.rpush(key, strings));
            }
        });
    }

    public Long lpush(final byte[] key, final byte[]... strings) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lpush(key, strings));
            }
        });
    }

    public Long llen(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.llen(key));
            }
        });
    }

    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.lrange(key, start, end));
            }
        });
    }

    public String ltrim(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.ltrim(key, start, end));
            }
        });
    }

    public byte[] lindex(final byte[] key, final long index) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.lindex(key, index));
            }
        });
    }

    public String lset(final byte[] key, final long index, final byte[] value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.lset(key, index, value));
            }
        });
    }

    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lrem(key, count, value));
            }
        });
    }

    public byte[] lpop(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.lpop(key));
            }
        });
    }

    public byte[] rpop(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.rpop(key));
            }
        });
    }

    public byte[] rpoplpush(final byte[] srckey, final byte[] dstkey) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.rpoplpush(srckey, dstkey));
            }
        });
    }

    public Long sadd(final byte[] key, final byte[]... members) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sadd(key, members));
            }
        });
    }

    public Set<byte[]> smembers(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.smembers(key));
            }
        });
    }

    public Long srem(final byte[] key, final byte[]... member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.srem(key, member));
            }
        });
    }

    public byte[] spop(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.spop(key));
            }
        });
    }

    public Long smove(final byte[] srckey, final byte[] dstkey, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.smove(srckey, dstkey, member));
            }
        });
    }

    public Long scard(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.scard(key));
            }
        });
    }

    public Boolean sismember(final byte[] key, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.sismember(key, member));
            }
        });
    }

    public Set<byte[]> sinter(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.sinter(keys));
            }
        });
    }

    public Long sinterstore(final byte[] dstkey, final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sinterstore(dstkey, keys));
            }
        });
    }

    public Set<byte[]> sunion(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.sunion(keys));
            }
        });
    }

    public Long sunionstore(final byte[] dstkey, final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sunionstore(dstkey, keys));
            }
        });
    }

    public Set<byte[]> sdiff(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.sdiff(keys));
            }
        });
    }

    public Long sdiffstore(final byte[] dstkey, final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sdiffstore(dstkey, keys));
            }
        });
    }

    public byte[] srandmember(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.srandmember(key));
            }
        });
    }

    public List<byte[]> srandmember(final byte[] key, final int count) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.srandmember(key, count));
            }
        });
    }

    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zadd(key, score, member));
            }
        });
    }

    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zadd(key, scoreMembers));
            }
        });
    }

    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrange(key, start, end));
            }
        });
    }

    public Long zrem(final byte[] key, final byte[]... members) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrem(key, members));
            }
        });
    }

    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.zincrby(key, score, member));
            }
        });
    }

    public Long zrank(final byte[] key, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrank(key, member));
            }
        });
    }

    public Long zrevrank(final byte[] key, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zrevrank(key, member));
            }
        });
    }

    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrevrange(key, start, end));
            }
        });
    }

    public Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeWithScores(key, start, end));
            }
        });
    }

    public Set<Tuple> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeWithScores(key, start, end));
            }
        });
    }

    public Long zcard(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcard(key));
            }
        });
    }

    public Double zscore(final byte[] key, final byte[] member) {
        return doAsynchronously(new AsyncOperation<Double>() {

            @Override
            public OperationResult<Double> execute(Jedis jedis) {
                return new OperationResult<Double>(jedis.zscore(key, member));
            }
        });
    }

    public void connect() {
        doAsynchronously(new AsyncOperation<Void>() {

            @Override
            public OperationResult<Void> execute(Jedis jedis) {
                jedis.connect();
                return null;
            }
        });
    }

    public List<byte[]> sort(final byte[] key) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.sort(key));
            }
        });
    }

    public List<byte[]> sort(final byte[] key, final SortingParams sortingParameters) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.sort(key, sortingParameters));
            }
        });
    }

    public List<byte[]> blpop(final int timeout, final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.blpop(timeout, keys));
            }
        });
    }

    public Long sort(final byte[] key, final SortingParams sortingParameters, final byte[] dstkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sort(key, sortingParameters, dstkey));
            }
        });
    }

    public Long sort(final byte[] key, final byte[] dstkey) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.sort(key, dstkey));
            }
        });
    }

    public List<byte[]> brpop(final int timeout, final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.brpop(timeout, keys));
            }
        });
    }

    public String auth(final String password) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.auth(password));
            }
        });
    }

    public Long zcount(final byte[] key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcount(key, min, max));
            }
        });
    }

    public Long zcount(final byte[] key, final byte[] min, final byte[] max) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zcount(key, min, max));
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrangeByScore(key, min, max));
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrangeByScore(key, min, max));
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
                                     final int count) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrangeByScore(key, min, max, offset, count));
            }
        });
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                     final int count) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrangeByScore(key, min, max, offset, count));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final double min, final double max, final int offset,
                                              final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
            }
        });
    }

    public Set<Tuple> zrangeByScoreWithScores(final byte[] key, final byte[] min, final byte[] max, final int offset,
                                              final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrangeByScoreWithScores(key, min, max, offset, count));
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrevrangeByScore(key, max, min));
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrevrangeByScore(key, max, min));
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min, final int offset,
                                        final int count) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrevrangeByScore(key, max, min, offset, count));
            }
        });
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min, final int offset,
                                        final int count) {
        return doAsynchronously(new AsyncOperation<Set<byte[]>>() {

            @Override
            public OperationResult<Set<byte[]>> execute(Jedis jedis) {
                return new OperationResult<Set<byte[]>>(jedis.zrevrangeByScore(key, max, min, offset, count));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
                                                 final int offset, final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min));
            }
        });
    }

    public Set<Tuple> zrevrangeByScoreWithScores(final byte[] key, final byte[] max, final byte[] min,
                                                 final int offset, final int count) {
        return doAsynchronously(new AsyncOperation<Set<Tuple>>() {

            @Override
            public OperationResult<Set<Tuple>> execute(Jedis jedis) {
                return new OperationResult<Set<Tuple>>(jedis.zrevrangeByScoreWithScores(key, max, min, offset, count));
            }
        });
    }

    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByRank(key, start, end));
            }
        });
    }

    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByScore(key, start, end));
            }
        });
    }

    public Long zremrangeByScore(final byte[] key, final byte[] start, final byte[] end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zremrangeByScore(key, start, end));
            }
        });
    }

    public Long zunionstore(final byte[] dstkey, final byte[]... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zunionstore(dstkey, sets));
            }
        });
    }

    public Long zunionstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zunionstore(dstkey, params, sets));
            }
        });
    }

    public Long zinterstore(final byte[] dstkey, final byte[]... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zinterstore(dstkey, sets));
            }
        });
    }

    public Long zinterstore(final byte[] dstkey, final ZParams params, final byte[]... sets) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.zinterstore(dstkey, params, sets));
            }
        });
    }

    public String save() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.save());
            }
        });
    }

    public String bgsave() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.bgsave());
            }
        });
    }

    public String bgrewriteaof() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.bgrewriteaof());
            }
        });
    }

    public Long lastsave() {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.lastsave());
            }
        });
    }

    public String shutdown() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.shutdown());
            }
        });
    }

    public String info() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.info());
            }
        });
    }

    public String info(final String section) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.info(section));
            }
        });
    }

    public void monitor(final JedisMonitor jedisMonitor) {
        doAsynchronously(new AsyncOperation<Void>() {

            @Override
            public OperationResult<Void> execute(Jedis jedis) {
                jedis.monitor(jedisMonitor);
                return null;
            }
        });
    }

    public String slaveofNoOne() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.slaveofNoOne());
            }
        });
    }

    public List<byte[]> configGet(final byte[] pattern) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.configGet(pattern));
            }
        });
    }

    public String configResetStat() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.configResetStat());
            }
        });
    }

    public byte[] configSet(final byte[] parameter, final byte[] value) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.configSet(parameter, value));
            }
        });
    }

    public boolean isConnected() {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.isConnected());
            }
        });
    }

    public Long strlen(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.strlen(key));
            }
        });
    }

    public void sync() {
        doAsynchronously(new AsyncOperation<Void>() {

            @Override
            public OperationResult<Void> execute(Jedis jedis) {
                jedis.sync();
                return null;
            }
        });
    }

    public Long persist(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.persist(key));
            }
        });
    }

    public Long rpushx(final byte[] key, final byte[]... string) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.rpushx(key, string));
            }
        });
    }

    public byte[] echo(final byte[] string) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.echo(string));
            }
        });
    }

    public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.linsert(key, where, pivot, value));
            }
        });
    }

    public String debug(final DebugParams params) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.debug(params));
            }
        });
    }

    public Client getClient() {
        return doAsynchronously(new AsyncOperation<Client>() {

            @Override
            public OperationResult<Client> execute(Jedis jedis) {
                return new OperationResult<Client>(jedis.getClient());
            }
        });
    }

    public byte[] brpoplpush(final byte[] source, final byte[] destination, final int timeout) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.brpoplpush(source, destination, timeout));
            }
        });
    }

    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.setbit(key, offset, value));
            }
        });
    }

    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.setbit(key, offset, value));
            }
        });
    }

    public Boolean getbit(final byte[] key, final long offset) {
        return doAsynchronously(new AsyncOperation<Boolean>() {

            @Override
            public OperationResult<Boolean> execute(Jedis jedis) {
                return new OperationResult<Boolean>(jedis.getbit(key, offset));
            }
        });
    }

    public Long bitpos(final byte[] key, final boolean value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitpos(key, value));
            }
        });
    }

    public Long bitpos(final byte[] key, final boolean value, final BitPosParams params) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitpos(key, value, params));
            }
        });
    }

    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.setrange(key, offset, value));
            }
        });
    }

    public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.getrange(key, startOffset, endOffset));
            }
        });
    }

    public Object eval(final byte[] script, final List<byte[]> keys, final List<byte[]> args) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script, keys, args));
            }
        });
    }

    public Object eval(final byte[] script, final byte[] keyCount, final byte[]... params) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script, keyCount, params));
            }
        });
    }

    public Object eval(final byte[] script, final int keyCount, final byte[]... params) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script, keyCount, params));
            }
        });
    }

    public Object eval(final byte[] script) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.eval(script));
            }
        });
    }

    public Object evalsha(final byte[] sha1) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(sha1));
            }
        });
    }

    public Object evalsha(final byte[] sha1, final List<byte[]> keys, final List<byte[]> args) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(sha1, keys, args));
            }
        });
    }

    public Object evalsha(final byte[] sha1, final int keyCount, final byte[]... params) {
        return doAsynchronously(new AsyncOperation<Object>() {

            @Override
            public OperationResult<Object> execute(Jedis jedis) {
                return new OperationResult<Object>(jedis.evalsha(sha1, keyCount, params));
            }
        });
    }

    public String scriptFlush() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.scriptFlush());
            }
        });
    }

    public List<Long> scriptExists(final byte[]... sha1) {
        return doAsynchronously(new AsyncOperation<List<Long>>() {

            @Override
            public OperationResult<List<Long>> execute(Jedis jedis) {
                return new OperationResult<List<Long>>(jedis.scriptExists(sha1));
            }
        });
    }

    public byte[] scriptLoad(final byte[] script) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.scriptLoad(script));
            }
        });
    }

    public String scriptKill() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.scriptKill());
            }
        });
    }

    public String slowlogReset() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.slowlogReset());
            }
        });
    }

    public Long slowlogLen() {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.slowlogLen());
            }
        });
    }

    public List<byte[]> slowlogGetBinary() {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.slowlogGetBinary());
            }
        });
    }

    public List<byte[]> slowlogGetBinary(final long entries) {
        return doAsynchronously(new AsyncOperation<List<byte[]>>() {

            @Override
            public OperationResult<List<byte[]>> execute(Jedis jedis) {
                return new OperationResult<List<byte[]>>(jedis.slowlogGetBinary(entries));
            }
        });
    }

    public Long objectRefcount(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.objectRefcount(key));
            }
        });
    }

    public byte[] objectEncoding(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.objectEncoding(key));
            }
        });
    }

    public Long objectIdletime(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.objectIdletime(key));
            }
        });
    }

    public Long bitcount(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitcount(key));
            }
        });
    }

    public Long bitcount(final byte[] key, final long start, final long end) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitcount(key, start, end));
            }
        });
    }

    public Long bitop(final BitOP op, final byte[] destKey, final byte[]... srcKeys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.bitop(op, destKey, srcKeys));
            }
        });
    }

    public byte[] dump(final byte[] key) {
        return doAsynchronously(new AsyncOperation<byte[]>() {

            @Override
            public OperationResult<byte[]> execute(Jedis jedis) {
                return new OperationResult<byte[]>(jedis.dump(key));
            }
        });
    }

    public String restore(final byte[] key, final int ttl, final byte[] serializedValue) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.restore(key, ttl, serializedValue));
            }
        });
    }

    @Deprecated
    public Long pexpire(final byte[] key, final int milliseconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpire(key, milliseconds));
            }
        });
    }

    public Long pexpire(final byte[] key, final long milliseconds) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpire(key, milliseconds));
            }
        });
    }

    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pexpireAt(key, millisecondsTimestamp));
            }
        });
    }

    public Long pttl(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pttl(key));
            }
        });
    }

    public String psetex(final byte[] key, final int milliseconds, final byte[] value) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.psetex(key, milliseconds, value));
            }
        });
    }

    public String set(final byte[] key, final byte[] value, final byte[] nxxx) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx));
            }
        });
    }

    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final int time) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.set(key, value, nxxx, expx, time));
            }
        });
    }

    public String clientKill(final byte[] client) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientKill(client));
            }
        });
    }

    public String clientGetname() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientGetname());
            }
        });
    }

    public String clientList() {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientList());
            }
        });
    }

    public String clientSetname(final byte[] name) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.clientSetname(name));
            }
        });
    }

    public List<String> time() {
        return doAsynchronously(new AsyncOperation<List<String>>() {

            @Override
            public OperationResult<List<String>> execute(Jedis jedis) {
                return new OperationResult<List<String>>(jedis.time());
            }
        });
    }

    public String migrate(final byte[] host, final int port, final byte[] key, final int destinationDb,
                          final int timeout) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.migrate(host, port, key, destinationDb, timeout));
            }
        });
    }

    public Long waitReplicas(final int replicas, final long timeout) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.waitReplicas(replicas, timeout));
            }
        });
    }

    public Long pfadd(final String key, final String... elements) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfadd(key, elements));
            }
        });
    }

    public Long pfadd(final byte[] key, final byte[]... elements) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfadd(key, elements));
            }
        });
    }

    public long pfcount(final byte[] key) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfcount(key));
            }
        });
    }

    public String pfmerge(final byte[] destkey, final byte[]... sourcekeys) {
        return doAsynchronously(new AsyncOperation<String>() {

            @Override
            public OperationResult<String> execute(Jedis jedis) {
                return new OperationResult<String>(jedis.pfmerge(destkey, sourcekeys));
            }
        });
    }

    public Long pfcount(final byte[]... keys) {
        return doAsynchronously(new AsyncOperation<Long>() {

            @Override
            public OperationResult<Long> execute(Jedis jedis) {
                return new OperationResult<Long>(jedis.pfcount(keys));
            }
        });
    }

    public ScanResult<byte[]> scan(final byte[] cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<byte[]>>() {

            @Override
            public OperationResult<ScanResult<byte[]>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<byte[]>>(jedis.scan(cursor));
            }
        });
    }

    public ScanResult<byte[]> scan(final byte[] cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<byte[]>>() {

            @Override
            public OperationResult<ScanResult<byte[]>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<byte[]>>(jedis.scan(cursor, params));
            }
        });
    }

    public ScanResult<Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<byte[], byte[]>>>() {

            @Override
            public OperationResult<ScanResult<Entry<byte[], byte[]>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<byte[], byte[]>>>(jedis.hscan(key, cursor));
            }
        });
    }

    public ScanResult<Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Entry<byte[], byte[]>>>() {

            @Override
            public OperationResult<ScanResult<Entry<byte[], byte[]>>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Entry<byte[], byte[]>>>(jedis.hscan(key, cursor, params));
            }
        });
    }

    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<byte[]>>() {

            @Override
            public OperationResult<ScanResult<byte[]>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<byte[]>>(jedis.sscan(key, cursor));
            }
        });
    }

    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<byte[]>>() {

            @Override
            public OperationResult<ScanResult<byte[]>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<byte[]>>(jedis.sscan(key, cursor, params));
            }
        });
    }

    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor));
            }
        });
    }

    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return doAsynchronously(new AsyncOperation<ScanResult<Tuple>>() {

            @Override
            public OperationResult<ScanResult<Tuple>> execute(Jedis jedis) {
                return new OperationResult<ScanResult<Tuple>>(jedis.zscan(key, cursor, params));
            }
        });
    }

}
