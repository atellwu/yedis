package com.yeahmobi.yedis.group;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.util.Slowlog;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.atomic.Yedis;
import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.common.YedisException;
import com.yeahmobi.yedis.loadbalance.LoadBalancer;
import com.yeahmobi.yedis.loadbalance.RandomLoadBalancer;
import com.yeahmobi.yedis.loadbalance.RoundRobinLoadBalancer;

@SuppressWarnings("deprecation")
public final class GroupYedis implements ConfigChangeListener {

    private final static Logger logger = LoggerFactory.getLogger(GroupYedis.class);

    static class MsHolder {

        LoadBalancer loadBalancer;
        Yedis        master;
        List<Yedis>  slaves;
    }

    private volatile MsHolder msHolder = new MsHolder();

    private final GroupConfig groupConfig;

    private AtomicBoolean     shutdown = new AtomicBoolean(false);

    public GroupYedis(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;

        msHolder.master = new Yedis(groupConfig.getMasterAtomConfig());
        msHolder.slaves = createYedis(groupConfig.getSlaveAtomConfigs());

        msHolder.loadBalancer = createLoadBalancer(msHolder.slaves, groupConfig.getLoadBalancerType());

        this.groupConfig.addListener(this);
    }

    private LoadBalancer createLoadBalancer(List<Yedis> slaves, LoadBalancer.Type loadBalanceType) {
        LoadBalancer loadBalancer = null;
        if (slaves != null && slaves.size() > 0) {
            switch (loadBalanceType) {
                case RANDOM:
                    loadBalancer = new RandomLoadBalancer(slaves);
                    break;
                case ROUND_ROBIN:
                    loadBalancer = new RoundRobinLoadBalancer(slaves);
                    break;
                default:
                    loadBalancer = new RoundRobinLoadBalancer(slaves);
                    break;
            }
        }
        return loadBalancer;
    }

    private List<Yedis> createYedis(List<AtomConfig> configs) {
        List<Yedis> list = new ArrayList<Yedis>();

        for (AtomConfig config : configs) {
            Yedis yedis = new Yedis(config);
            list.add(yedis);
        }
        return list;
    }

    private Yedis getYedis(boolean readonly) {
        // long start = System.nanoTime();
        checkClose();
        Yedis re = null;
        if (readonly) {
            switch (groupConfig.getReadMode()) {
                case SLAVEPREFERRED:
                    if (msHolder.loadBalancer != null) {
                        re = msHolder.loadBalancer.route();
                    } else {
                        re = msHolder.master;
                    }
                    break;
                case SLAVE:
                    if (msHolder.loadBalancer != null) {
                        re = msHolder.loadBalancer.route();
                    }
                    break;
                case MASTERPREFERRED:
                    if (msHolder.master != null) {
                        re = msHolder.master;
                    } else if (msHolder.loadBalancer != null) {
                        re = msHolder.loadBalancer.route();
                    }
                    break;
                case MASTER:
                    re = msHolder.master;
                    break;
            }
            if (re == null) {
                throw new YedisException("ReadMode is " + groupConfig.getReadMode()
                                         + ", but no suitable slave server available.");
            }
            // System.out.println(System.nanoTime() - start);
            return re;
        }
        if (msHolder.master != null) {
            return msHolder.master;
        } else {
            throw new YedisException("No suitable master server available.");
        }
    }

    private void checkClose() {
        if (shutdown.get()) {
            throw new IllegalStateException("Already closed.");
        }
    }

    @Override
    public synchronized void onChanged(MasterSlaveConfigManager configManager) {
        // 如果已经关闭，则忽略配置变更
        if (shutdown.get()) {
            return;
        }

        MsHolder newMsHolder = new MsHolder();
        Yedis unusedMaster = null;
        List<Yedis> unusedSlaves = null;
        try {

            // 处理master的变化
            try {
                AtomConfig masterConfig = groupConfig.getMasterAtomConfig();
                if (!msHolder.master.getConfig().getServerInfo().equals(masterConfig.getServerInfo())) {
                    newMsHolder.master = new Yedis(groupConfig.getMasterAtomConfig());
                    logger.info("Master changed to " + groupConfig.getMasterAtomConfig().getHost() + ":"
                                + groupConfig.getMasterAtomConfig().getPort());
                    unusedMaster = msHolder.master;
                } else {
                    newMsHolder.master = msHolder.master;
                }
            } catch (Exception e) {
                // 如果有异常，则需要关闭刚才新建的实例
                unusedMaster = newMsHolder.master;
                throw e;
            }

            // 处理slave的变化:构造slaves，从已有yedis中复用，最后，没被复用的则close
            List<Yedis> createdSlaves = new ArrayList<Yedis>();
            try {
                List<AtomConfig> slaveConfigs = groupConfig.getSlaveAtomConfigs();
                if (slaveConfigs != null) {
                    List<Yedis> oldSlaves = new ArrayList<Yedis>(msHolder.slaves);
                    List<Yedis> newSlaves = new ArrayList<Yedis>(slaveConfigs.size());
                    for (AtomConfig config : slaveConfigs) {
                        Yedis existsYedis = takeSlaveIfExists(oldSlaves, config.getServerInfo());
                        if (existsYedis != null) {
                            // 复用已经存在的Yedis
                            newSlaves.add(existsYedis);
                        } else {
                            // 是新的server，则构建Yedis
                            Yedis yedis = new Yedis(config);
                            newSlaves.add(yedis);
                            createdSlaves.add(yedis);
                            logger.info("New slave created: " + yedis.getConfig().getHost() + ":"
                                        + yedis.getConfig().getPort());
                        }
                    }
                    newMsHolder.loadBalancer = createLoadBalancer(newSlaves, groupConfig.getLoadBalancerType());
                    newMsHolder.slaves = newSlaves;
                    unusedSlaves = oldSlaves;
                }
            } catch (Exception e) {
                // 如果有异常，则需要关闭刚才新建的实例
                unusedSlaves = createdSlaves;
                throw e;
            }

            // 切换
            this.msHolder = newMsHolder;

        } catch (Exception e) {
            logger.error("Master/slave Config changed, but error occurred, so GroupYedis haven't switched connection: ",
                         e);
        }

        // 释放无用的yedis
        if (unusedMaster != null) {
            unusedMaster.close();
            logger.info("Unused master closed: " + unusedMaster.getConfig().getHost() + ":"
                        + unusedMaster.getConfig().getPort());
        }
        if (unusedSlaves != null) {
            for (Yedis yedis : unusedSlaves) {
                yedis.close();
                logger.info("Unused slave closed: " + yedis.getConfig().getHost() + ":" + yedis.getConfig().getPort());
            }
        }
    }

    private Yedis takeSlaveIfExists(List<Yedis> oldYedisList, ServerInfo serverInfo) {
        Iterator<Yedis> iterator = oldYedisList.iterator();
        while (iterator.hasNext()) {
            Yedis yedis = iterator.next();
            if (yedis.getConfig().getServerInfo().equals(serverInfo)) {
                iterator.remove();
                return yedis;
            }
        }
        return null;
    }

    public void close() {
        if (shutdown.compareAndSet(false, true)) {

            synchronized (this) {
                // 关闭slave和master
                msHolder.master.close();
                for (Yedis slave : msHolder.slaves) {
                    slave.close();
                }
            }

            groupConfig.getMasterSlaveConfigManager().close();

        }
    }

    public String set(String key, String value) {
        return getYedis(false).set(key, value);
    }

    public String set(String key, String value, String nxxx, String expx, long time) {
        return getYedis(false).set(key, value, nxxx, expx, time);
    }

    public String get(String key) {
        return getYedis(true).get(key);
    }

    public Boolean exists(String key) {
        return getYedis(true).exists(key);
    }

    public Long del(String... keys) {
        return getYedis(false).del(keys);
    }

    public Long del(String key) {
        return getYedis(false).del(key);
    }

    public String type(String key) {
        return getYedis(true).type(key);
    }

    public Set<String> keys(String pattern) {
        return getYedis(true).keys(pattern);
    }

    public String randomKey() {
        return getYedis(true).randomKey();
    }

    public String rename(String oldkey, String newkey) {
        return getYedis(false).rename(oldkey, newkey);
    }

    public Long renamenx(String oldkey, String newkey) {
        return getYedis(false).renamenx(oldkey, newkey);
    }

    public Long expire(String key, int seconds) {
        return getYedis(false).expire(key, seconds);
    }

    public Long expireAt(String key, long unixTime) {
        return getYedis(false).expireAt(key, unixTime);
    }

    public Long ttl(String key) {
        return getYedis(true).ttl(key);
    }

    public Long move(String key, int dbIndex) {
        return getYedis(false).move(key, dbIndex);
    }

    public String getSet(String key, String value) {
        return getYedis(false).getSet(key, value);
    }

    public List<String> mget(String... keys) {
        return getYedis(true).mget(keys);
    }

    public Long setnx(String key, String value) {
        return getYedis(false).setnx(key, value);
    }

    public String setex(String key, int seconds, String value) {
        return getYedis(false).setex(key, seconds, value);
    }

    public String mset(String... keysvalues) {
        return getYedis(false).mset(keysvalues);
    }

    public Long msetnx(String... keysvalues) {
        return getYedis(false).msetnx(keysvalues);
    }

    public Long decrBy(String key, long integer) {
        return getYedis(false).decrBy(key, integer);
    }

    public Long decr(String key) {
        return getYedis(false).decr(key);
    }

    public Long incrBy(String key, long integer) {
        return getYedis(false).incrBy(key, integer);
    }

    public Double incrByFloat(String key, double value) {
        return getYedis(false).incrByFloat(key, value);
    }

    public Long incr(String key) {
        return getYedis(false).incr(key);
    }

    public Long append(String key, String value) {
        return getYedis(false).append(key, value);
    }

    public String substr(String key, int start, int end) {
        return getYedis(false).substr(key, start, end);
    }

    public Long hset(String key, String field, String value) {
        return getYedis(false).hset(key, field, value);
    }

    public String hget(String key, String field) {
        return getYedis(true).hget(key, field);
    }

    public Long hsetnx(String key, String field, String value) {
        return getYedis(false).hsetnx(key, field, value);
    }

    public String hmset(String key, Map<String, String> hash) {
        return getYedis(false).hmset(key, hash);
    }

    public List<String> hmget(String key, String... fields) {
        return getYedis(true).hmget(key, fields);
    }

    public Long hincrBy(String key, String field, long value) {
        return getYedis(false).hincrBy(key, field, value);
    }

    public Double hincrByFloat(String key, String field, double value) {
        return getYedis(false).hincrByFloat(key, field, value);
    }

    public Boolean hexists(String key, String field) {
        return getYedis(false).hexists(key, field);
    }

    public Long hdel(String key, String... fields) {
        return getYedis(false).hdel(key, fields);
    }

    public Long hlen(String key) {
        return getYedis(false).hlen(key);
    }

    public Set<String> hkeys(String key) {
        return getYedis(false).hkeys(key);
    }

    public List<String> hvals(String key) {
        return getYedis(false).hvals(key);
    }

    public Map<String, String> hgetAll(String key) {
        return getYedis(false).hgetAll(key);
    }

    public Long rpush(String key, String... strings) {
        return getYedis(false).rpush(key, strings);
    }

    public Long lpush(String key, String... strings) {
        return getYedis(false).lpush(key, strings);
    }

    public Long llen(String key) {
        return getYedis(false).llen(key);
    }

    public List<String> lrange(String key, long start, long end) {
        return getYedis(false).lrange(key, start, end);
    }

    public String ltrim(String key, long start, long end) {
        return getYedis(false).ltrim(key, start, end);
    }

    public String lindex(String key, long index) {
        return getYedis(false).lindex(key, index);
    }

    public String lset(String key, long index, String value) {
        return getYedis(false).lset(key, index, value);
    }

    public Long lrem(String key, long count, String value) {
        return getYedis(false).lrem(key, count, value);
    }

    public String lpop(String key) {
        return getYedis(false).lpop(key);
    }

    public String rpop(String key) {
        return getYedis(false).rpop(key);
    }

    public String rpoplpush(String srckey, String dstkey) {
        return getYedis(false).rpoplpush(srckey, dstkey);
    }

    public Long sadd(String key, String... members) {
        return getYedis(false).sadd(key, members);
    }

    public Set<String> smembers(String key) {
        return getYedis(false).smembers(key);
    }

    public Long srem(String key, String... members) {
        return getYedis(false).srem(key, members);
    }

    public String spop(String key) {
        return getYedis(false).spop(key);
    }

    public Long smove(String srckey, String dstkey, String member) {
        return getYedis(false).smove(srckey, dstkey, member);
    }

    public Long scard(String key) {
        return getYedis(false).scard(key);
    }

    public Boolean sismember(String key, String member) {
        return getYedis(false).sismember(key, member);
    }

    public Set<String> sinter(String... keys) {
        return getYedis(false).sinter(keys);
    }

    public Long sinterstore(String dstkey, String... keys) {
        return getYedis(false).sinterstore(dstkey, keys);
    }

    public Set<String> sunion(String... keys) {
        return getYedis(false).sunion(keys);
    }

    public Long sunionstore(String dstkey, String... keys) {
        return getYedis(false).sunionstore(dstkey, keys);
    }

    public Set<String> sdiff(String... keys) {
        return getYedis(false).sdiff(keys);
    }

    public Long sdiffstore(String dstkey, String... keys) {
        return getYedis(false).sdiffstore(dstkey, keys);
    }

    public String srandmember(String key) {
        return getYedis(false).srandmember(key);
    }

    public List<String> srandmember(String key, int count) {
        return getYedis(false).srandmember(key, count);
    }

    public Long zadd(String key, double score, String member) {
        return getYedis(false).zadd(key, score, member);
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return getYedis(false).zadd(key, scoreMembers);
    }

    public Set<String> zrange(String key, long start, long end) {
        return getYedis(false).zrange(key, start, end);
    }

    public Long zrem(String key, String... members) {
        return getYedis(false).zrem(key, members);
    }

    public Double zincrby(String key, double score, String member) {
        return getYedis(false).zincrby(key, score, member);
    }

    public Long zrank(String key, String member) {
        return getYedis(false).zrank(key, member);
    }

    public Long zrevrank(String key, String member) {
        return getYedis(false).zrevrank(key, member);
    }

    public Set<String> zrevrange(String key, long start, long end) {
        return getYedis(false).zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return getYedis(false).zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return getYedis(false).zrevrangeWithScores(key, start, end);
    }

    public Long zcard(String key) {
        return getYedis(false).zcard(key);
    }

    public Double zscore(String key, String member) {
        return getYedis(false).zscore(key, member);
    }

    public List<String> sort(String key) {
        return getYedis(false).sort(key);
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        return getYedis(false).sort(key, sortingParameters);
    }

    public List<String> blpop(int timeout, String... keys) {
        return getYedis(false).blpop(timeout, keys);
    }

    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return getYedis(false).sort(key, sortingParameters, dstkey);
    }

    public Long sort(String key, String dstkey) {
        return getYedis(false).sort(key, dstkey);
    }

    public List<String> brpop(int timeout, String... keys) {
        return getYedis(false).brpop(timeout, keys);
    }

    public Long zcount(String key, double min, double max) {
        return getYedis(false).zcount(key, min, max);
    }

    public Long zcount(String key, String min, String max) {
        return getYedis(false).zcount(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        return getYedis(false).zrangeByScore(key, min, max);
    }

    public Set<String> zrangeByScore(String key, String min, String max) {
        return getYedis(false).zrangeByScore(key, min, max);
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return getYedis(false).zrangeByScore(key, min, max, offset, count);
    }

    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return getYedis(false).zrangeByScore(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return getYedis(false).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return getYedis(false).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return getYedis(false).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return getYedis(false).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return getYedis(false).zrevrangeByScore(key, max, min);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return getYedis(false).zrevrangeByScore(key, max, min);
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return getYedis(false).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return getYedis(false).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return getYedis(false).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return getYedis(false).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return getYedis(false).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return getYedis(false).zrevrangeByScoreWithScores(key, max, min);
    }

    public Long zremrangeByRank(String key, long start, long end) {
        return getYedis(false).zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(String key, double start, double end) {
        return getYedis(false).zremrangeByScore(key, start, end);
    }

    public Long zremrangeByScore(String key, String start, String end) {
        return getYedis(false).zremrangeByScore(key, start, end);
    }

    public Long zunionstore(String dstkey, String... sets) {
        return getYedis(false).zunionstore(dstkey, sets);
    }

    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return getYedis(false).zunionstore(dstkey, params, sets);
    }

    public Long zinterstore(String dstkey, String... sets) {
        return getYedis(false).zinterstore(dstkey, sets);
    }

    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return getYedis(false).zinterstore(dstkey, params, sets);
    }

    public Long strlen(String key) {
        return getYedis(false).strlen(key);
    }

    public Long lpushx(String key, String... string) {
        return getYedis(false).lpushx(key, string);
    }

    public Long persist(String key) {
        return getYedis(false).persist(key);
    }

    public Long rpushx(String key, String... string) {
        return getYedis(false).rpushx(key, string);
    }

    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return getYedis(false).linsert(key, where, pivot, value);
    }

    public String brpoplpush(String source, String destination, int timeout) {
        return getYedis(false).brpoplpush(source, destination, timeout);
    }

    public Boolean setbit(String key, long offset, boolean value) {
        return getYedis(false).setbit(key, offset, value);
    }

    public Boolean setbit(String key, long offset, String value) {
        return getYedis(false).setbit(key, offset, value);
    }

    public Boolean getbit(String key, long offset) {
        return getYedis(false).getbit(key, offset);
    }

    public Long setrange(String key, long offset, String value) {
        return getYedis(false).setrange(key, offset, value);
    }

    public String getrange(String key, long startOffset, long endOffset) {
        return getYedis(false).getrange(key, startOffset, endOffset);
    }

    public Long bitpos(String key, boolean value) {
        return getYedis(false).bitpos(key, value);
    }

    public Long bitpos(String key, boolean value, BitPosParams params) {
        return getYedis(false).bitpos(key, value, params);
    }

    public Object eval(String script, int keyCount, String... params) {
        return getYedis(false).eval(script, keyCount, params);
    }

    public Object eval(String script, List<String> keys, List<String> args) {
        return getYedis(false).eval(script, keys, args);
    }

    public Object eval(String script) {
        return getYedis(false).eval(script);
    }

    public Object evalsha(String script) {
        return getYedis(false).evalsha(script);
    }

    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        return getYedis(false).evalsha(sha1, keys, args);
    }

    public Object evalsha(String sha1, int keyCount, String... params) {
        return getYedis(false).evalsha(sha1, keyCount, params);
    }

    public Boolean scriptExists(String sha1) {
        return getYedis(false).scriptExists(sha1);
    }

    public List<Boolean> scriptExists(String... sha1) {
        return getYedis(false).scriptExists(sha1);
    }

    public String scriptLoad(String script) {
        return getYedis(false).scriptLoad(script);
    }

    public List<Slowlog> slowlogGet() {
        return getYedis(false).slowlogGet();
    }

    public List<Slowlog> slowlogGet(long entries) {
        return getYedis(false).slowlogGet(entries);
    }

    public Long bitcount(String key) {
        return getYedis(false).bitcount(key);
    }

    public Long bitcount(String key, long start, long end) {
        return getYedis(false).bitcount(key, start, end);
    }

    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        return getYedis(false).bitop(op, destKey, srcKeys);
    }

    public byte[] dump(String key) {
        return getYedis(false).dump(key);
    }

    public String restore(String key, int ttl, byte[] serializedValue) {
        return getYedis(false).restore(key, ttl, serializedValue);
    }

    public Long pexpire(String key, int milliseconds) {
        return getYedis(false).pexpire(key, milliseconds);
    }

    public Long pexpire(String key, long milliseconds) {
        return getYedis(false).pexpire(key, milliseconds);
    }

    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return getYedis(false).pexpireAt(key, millisecondsTimestamp);
    }

    public Long pttl(String key) {
        return getYedis(true).pttl(key);
    }

    public String psetex(String key, int milliseconds, String value) {
        return getYedis(false).psetex(key, milliseconds, value);
    }

    public String set(String key, String value, String nxxx) {
        return getYedis(false).set(key, value, nxxx);
    }

    public String set(String key, String value, String nxxx, String expx, int time) {
        return getYedis(false).set(key, value, nxxx, expx, time);
    }

    public Long pfadd(String key, String... elements) {
        return getYedis(false).pfadd(key, elements);
    }

    public long pfcount(String key) {
        return getYedis(false).pfcount(key);
    }

    public long pfcount(String... keys) {
        return getYedis(false).pfcount(keys);
    }

    public String pfmerge(String destkey, String... sourcekeys) {
        return getYedis(false).pfmerge(destkey, sourcekeys);
    }

    public String set(byte[] key, byte[] value) {
        return getYedis(false).set(key, value);
    }

    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
        return getYedis(false).set(key, value, nxxx, expx, time);
    }

    public byte[] get(byte[] key) {
        return getYedis(true).get(key);
    }

    public Boolean exists(byte[] key) {
        return getYedis(false).exists(key);
    }

    public Long del(byte[]... keys) {
        return getYedis(false).del(keys);
    }

    public Long del(byte[] key) {
        return getYedis(false).del(key);
    }

    public String type(byte[] key) {
        return getYedis(false).type(key);
    }

    public String flushDB() {
        return getYedis(false).flushDB();
    }

    public Set<byte[]> keys(byte[] pattern) {
        return getYedis(false).keys(pattern);
    }

    public byte[] randomBinaryKey() {
        return getYedis(false).randomBinaryKey();
    }

    public String rename(byte[] oldkey, byte[] newkey) {
        return getYedis(false).rename(oldkey, newkey);
    }

    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return getYedis(false).renamenx(oldkey, newkey);
    }

    public Long dbSize() {
        return getYedis(false).dbSize();
    }

    public Long expire(byte[] key, int seconds) {
        return getYedis(false).expire(key, seconds);
    }

    public Long expireAt(byte[] key, long unixTime) {
        return getYedis(false).expireAt(key, unixTime);
    }

    public Long ttl(byte[] key) {
        return getYedis(true).ttl(key);
    }

    public Long move(byte[] key, int dbIndex) {
        return getYedis(false).move(key, dbIndex);
    }

    public String flushAll() {
        return getYedis(false).flushAll();
    }

    public byte[] getSet(byte[] key, byte[] value) {
        return getYedis(false).getSet(key, value);
    }

    public List<byte[]> mget(byte[]... keys) {
        return getYedis(true).mget(keys);
    }

    public Long setnx(byte[] key, byte[] value) {
        return getYedis(false).setnx(key, value);
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        return getYedis(false).setex(key, seconds, value);
    }

    public String mset(byte[]... keysvalues) {
        return getYedis(false).mset(keysvalues);
    }

    public Long msetnx(byte[]... keysvalues) {
        return getYedis(false).msetnx(keysvalues);
    }

    public Long decrBy(byte[] key, long integer) {
        return getYedis(false).decrBy(key, integer);
    }

    public Long decr(byte[] key) {
        return getYedis(false).decr(key);
    }

    public Long incrBy(byte[] key, long integer) {
        return getYedis(false).incrBy(key, integer);
    }

    public Double incrByFloat(byte[] key, double integer) {
        return getYedis(false).incrByFloat(key, integer);
    }

    public Long incr(byte[] key) {
        return getYedis(false).incr(key);
    }

    public Long append(byte[] key, byte[] value) {
        return getYedis(false).append(key, value);
    }

    public byte[] substr(byte[] key, int start, int end) {
        return getYedis(false).substr(key, start, end);
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        return getYedis(false).hset(key, field, value);
    }

    public byte[] hget(byte[] key, byte[] field) {
        return getYedis(true).hget(key, field);
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return getYedis(false).hsetnx(key, field, value);
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return getYedis(false).hmset(key, hash);
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return getYedis(true).hmget(key, fields);
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        return getYedis(false).hincrBy(key, field, value);
    }

    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return getYedis(false).hincrByFloat(key, field, value);
    }

    public Boolean hexists(byte[] key, byte[] field) {
        return getYedis(true).hexists(key, field);
    }

    public Long hdel(byte[] key, byte[]... fields) {
        return getYedis(false).hdel(key, fields);
    }

    public Long hlen(byte[] key) {
        return getYedis(true).hlen(key);
    }

    public Set<byte[]> hkeys(byte[] key) {
        return getYedis(true).hkeys(key);
    }

    public List<byte[]> hvals(byte[] key) {
        return getYedis(true).hvals(key);
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return getYedis(true).hgetAll(key);
    }

    public Long rpush(byte[] key, byte[]... strings) {
        return getYedis(false).rpush(key, strings);
    }

    public Long lpush(byte[] key, byte[]... strings) {
        return getYedis(false).lpush(key, strings);
    }

    public Long llen(byte[] key) {
        return getYedis(true).llen(key);
    }

    public List<byte[]> lrange(byte[] key, long start, long end) {
        return getYedis(true).lrange(key, start, end);
    }

    public String ltrim(byte[] key, long start, long end) {
        return getYedis(false).ltrim(key, start, end);
    }

    public byte[] lindex(byte[] key, long index) {
        return getYedis(true).lindex(key, index);
    }

    public String lset(byte[] key, long index, byte[] value) {
        return getYedis(false).lset(key, index, value);
    }

    public Long lrem(byte[] key, long count, byte[] value) {
        return getYedis(false).lrem(key, count, value);
    }

    public byte[] lpop(byte[] key) {
        return getYedis(false).lpop(key);
    }

    public byte[] rpop(byte[] key) {
        return getYedis(false).rpop(key);
    }

    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        return getYedis(false).rpoplpush(srckey, dstkey);
    }

    public Long sadd(byte[] key, byte[]... members) {
        return getYedis(false).sadd(key, members);
    }

    public Set<byte[]> smembers(byte[] key) {
        return getYedis(true).smembers(key);
    }

    public Long srem(byte[] key, byte[]... member) {
        return getYedis(false).srem(key, member);
    }

    public byte[] spop(byte[] key) {
        return getYedis(false).spop(key);
    }

    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return getYedis(false).smove(srckey, dstkey, member);
    }

    public Long scard(byte[] key) {
        return getYedis(true).scard(key);
    }

    public Boolean sismember(byte[] key, byte[] member) {
        return getYedis(true).sismember(key, member);
    }

    public Set<byte[]> sinter(byte[]... keys) {
        return getYedis(true).sinter(keys);
    }

    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return getYedis(false).sinterstore(dstkey, keys);
    }

    public Set<byte[]> sunion(byte[]... keys) {
        return getYedis(true).sunion(keys);
    }

    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        return getYedis(false).sunionstore(dstkey, keys);
    }

    public Set<byte[]> sdiff(byte[]... keys) {
        return getYedis(true).sdiff(keys);
    }

    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return getYedis(false).sdiffstore(dstkey, keys);
    }

    public byte[] srandmember(byte[] key) {
        return getYedis(true).srandmember(key);
    }

    public List<byte[]> srandmember(byte[] key, int count) {
        return getYedis(true).srandmember(key, count);
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        return getYedis(false).zadd(key, score, member);
    }

    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return getYedis(false).zadd(key, scoreMembers);
    }

    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return getYedis(true).zrange(key, start, end);
    }

    public Long zrem(byte[] key, byte[]... members) {
        return getYedis(false).zrem(key, members);
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        return getYedis(false).zincrby(key, score, member);
    }

    public Long zrank(byte[] key, byte[] member) {
        return getYedis(true).zrank(key, member);
    }

    public Long zrevrank(byte[] key, byte[] member) {
        return getYedis(true).zrevrank(key, member);
    }

    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return getYedis(true).zrevrange(key, start, end);
    }

    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return getYedis(true).zrangeWithScores(key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return getYedis(true).zrevrangeWithScores(key, start, end);
    }

    public Long zcard(byte[] key) {
        return getYedis(true).zcard(key);
    }

    public Double zscore(byte[] key, byte[] member) {
        return getYedis(true).zscore(key, member);
    }

    public List<byte[]> sort(byte[] key) {
        return getYedis(true).sort(key);
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return getYedis(true).sort(key, sortingParameters);
    }

    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return getYedis(false).blpop(timeout, keys);
    }

    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return getYedis(false).sort(key, sortingParameters, dstkey);
    }

    public Long sort(byte[] key, byte[] dstkey) {
        return getYedis(false).sort(key, dstkey);
    }

    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return getYedis(false).brpop(timeout, keys);
    }

    public Long zcount(byte[] key, double min, double max) {
        return getYedis(true).zcount(key, min, max);
    }

    public Long zcount(byte[] key, byte[] min, byte[] max) {
        return getYedis(true).zcount(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return getYedis(true).zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return getYedis(true).zrangeByScore(key, min, max);
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return getYedis(true).zrangeByScore(key, min, max, offset, count);
    }

    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return getYedis(true).zrangeByScore(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return getYedis(true).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return getYedis(true).zrangeByScoreWithScores(key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return getYedis(true).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return getYedis(true).zrangeByScoreWithScores(key, min, max, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return getYedis(true).zrevrangeByScore(key, max, min);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return getYedis(true).zrevrangeByScore(key, max, min);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return getYedis(true).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return getYedis(true).zrevrangeByScore(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return getYedis(true).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return getYedis(true).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return getYedis(true).zrevrangeByScoreWithScores(key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return getYedis(true).zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    public Long zremrangeByRank(byte[] key, long start, long end) {
        return getYedis(false).zremrangeByRank(key, start, end);
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        return getYedis(false).zremrangeByScore(key, start, end);
    }

    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return getYedis(false).zremrangeByScore(key, start, end);
    }

    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        return getYedis(false).zunionstore(dstkey, sets);
    }

    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return getYedis(false).zunionstore(dstkey, params, sets);
    }

    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        return getYedis(false).zinterstore(dstkey, sets);
    }

    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return getYedis(false).zinterstore(dstkey, params, sets);
    }

    public Long strlen(byte[] key) {
        return getYedis(true).strlen(key);
    }

    public Long lpushx(byte[] key, byte[]... string) {
        return getYedis(false).lpushx(key, string);
    }

    public Long persist(byte[] key) {
        return getYedis(false).persist(key);
    }

    public Long rpushx(byte[] key, byte[]... string) {
        return getYedis(false).rpushx(key, string);
    }

    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return getYedis(false).linsert(key, where, pivot, value);
    }

    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        return getYedis(false).brpoplpush(source, destination, timeout);
    }

    public Boolean setbit(byte[] key, long offset, boolean value) {
        return getYedis(false).setbit(key, offset, value);
    }

    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return getYedis(false).setbit(key, offset, value);
    }

    public Boolean getbit(byte[] key, long offset) {
        return getYedis(true).getbit(key, offset);
    }

    public Long bitpos(byte[] key, boolean value) {
        return getYedis(true).bitpos(key, value);
    }

    public Long bitpos(byte[] key, boolean value, BitPosParams params) {
        return getYedis(true).bitpos(key, value, params);
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        return getYedis(false).setrange(key, offset, value);
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        return getYedis(true).getrange(key, startOffset, endOffset);
    }

    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return getYedis(false).eval(script, keys, args);
    }

    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
        return getYedis(false).eval(script, keyCount, params);
    }

    public Object eval(byte[] script, int keyCount, byte[]... params) {
        return getYedis(false).eval(script, keyCount, params);
    }

    public Object eval(byte[] script) {
        return getYedis(false).eval(script);
    }

    public Object evalsha(byte[] sha1) {
        return getYedis(false).evalsha(sha1);
    }

    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        return getYedis(false).evalsha(sha1, keys, args);
    }

    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
        return getYedis(false).evalsha(sha1, keyCount, params);
    }

    public Long bitcount(byte[] key) {
        return getYedis(true).bitcount(key);
    }

    public Long bitcount(byte[] key, long start, long end) {
        return getYedis(true).bitcount(key, start, end);
    }

    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        return getYedis(false).bitop(op, destKey, srcKeys);
    }

    public byte[] dump(byte[] key) {
        return getYedis(true).dump(key);
    }

    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        return getYedis(false).restore(key, ttl, serializedValue);
    }

    @Deprecated
    public Long pexpire(byte[] key, int milliseconds) {
        return getYedis(false).pexpire(key, milliseconds);
    }

    public Long pexpire(byte[] key, long milliseconds) {
        return getYedis(false).pexpire(key, milliseconds);
    }

    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        return getYedis(false).pexpireAt(key, millisecondsTimestamp);
    }

    public Long pttl(byte[] key) {
        return getYedis(true).pttl(key);
    }

    public String psetex(byte[] key, int milliseconds, byte[] value) {
        return getYedis(false).psetex(key, milliseconds, value);
    }

    public String set(byte[] key, byte[] value, byte[] nxxx) {
        return getYedis(false).set(key, value, nxxx);
    }

    public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
        return getYedis(false).set(key, value, nxxx, expx, time);
    }

    public Long pfadd(byte[] key, byte[]... elements) {
        return getYedis(false).pfadd(key, elements);
    }

    public long pfcount(byte[] key) {
        return getYedis(true).pfcount(key);
    }

    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        return getYedis(false).pfmerge(destkey, sourcekeys);
    }

    public Long pfcount(byte[]... keys) {
        return getYedis(true).pfcount(keys);
    }

    public ScanResult<byte[]> scan(byte[] cursor) {
        return getYedis(true).scan(cursor);
    }

    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        return getYedis(true).scan(cursor, params);
    }

    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor) {
        return getYedis(true).hscan(key, cursor);
    }

    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        return getYedis(true).hscan(key, cursor, params);
    }

    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
        return getYedis(true).sscan(key, cursor);
    }

    public ScanResult<byte[]> sscan(byte[] key, byte[] cursor, ScanParams params) {
        return getYedis(true).sscan(key, cursor, params);
    }

    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
        return getYedis(true).zscan(key, cursor);
    }

    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
        return getYedis(false).zscan(key, cursor, params);
    }

}
