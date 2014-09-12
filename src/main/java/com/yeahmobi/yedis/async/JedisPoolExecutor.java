package com.yeahmobi.yedis.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.util.SleepStrategy;

public class JedisPoolExecutor {

    private static final Logger logger      = LoggerFactory.getLogger(JedisPoolExecutor.class);

    private static final String nameFormat  = "YedisPool(%s)-Client%d";

    private final AtomicInteger count       = new AtomicInteger(1);

    private final AtomicBoolean shutdown    = new AtomicBoolean(false);

    private final Worker[]      workers;

    private final AtomicInteger workerIndex = new AtomicInteger(0);

    private AtomConfig          config;

    public JedisPoolExecutor(AtomConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Argument cannot be null.");
        }

        this.config = config;

        this.workers = new Worker[config.getThreadPoolSize()];
    }

    public void start() {
        for (int i = 0; i < this.workers.length; i++) {
            this.workers[i] = new Worker();
            this.workers[i].start();
        }
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            for (Worker worker : workers) {
                worker.close();
            }
        }
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    private void checkClose() {
        if (shutdown.get()) {
            throw new IllegalStateException("It is already closed.");
        }
    }

    public <T> Future<OperationResult<T>> submit(AsyncOperation<T> opr) {
        checkClose();
        if (opr == null) throw new NullPointerException();
        FutureTask<OperationResult<T>> ftask = new FutureTask<OperationResult<T>>(opr);

        // 选择一个work, task入队
        workers[nextIndex()].addTask(ftask);

        return ftask;
    }

    private int nextIndex() {
        return Math.abs(workerIndex.getAndIncrement() % workers.length);
    }

    @SuppressWarnings("rawtypes")
    private class Worker extends Thread {

        private LinkedBlockingQueue<FutureTask> queue         = new LinkedBlockingQueue<FutureTask>();

        private Jedis                           jedis;

        private SleepStrategy                   sleepStrategy = new SleepStrategy();

        public Worker() {
            super(String.format(nameFormat, config.getHost() + ":" + config.getPort(), count.getAndIncrement()));
            this.setDaemon(true);
        }

        public void close() {
            FutureTask task;
            // 将queue中的 task拿出来，cancle掉
            while ((task = queue.poll()) != null) {
                task.cancel(true);
            }
            this.interrupt();
            if (jedis != null) {
                jedis.close();
            }
            logger.info("Closed from " + config.getHost() + ":" + config.getPort());
        }

        public void addTask(FutureTask task) {
            queue.add(task);
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                try {
                    checkJedis();

                    FutureTask task = queue.take();
                    if (!task.isCancelled()) {
                        ThreadLocalHolder.jedisHolder.set(jedis);
                        task.run();
                        task.get();// 尝试获取结果，以便获取task是否有异常，如果有异常，需要关闭jedis
                    }
                } catch (InterruptedException e) {
                    // 如果被中断，按逻辑会继续检查shutdown (Yedis超时cancle时也会走到此中断的逻辑)
                } catch (ExecutionException e) {
                    // 此处仅仅是为了判断task是否有jedis的网络异常
                    Throwable cause = e.getCause();
                    if (cause instanceof JedisConnectionException) {
                        // 网络问题，关闭
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                } catch (JedisConnectionException e) {
                    // 创建jedis时遇到网络问题
                    logger.error("Error when connecting to redis server in YedisExecutor: " + e.getMessage());
                    // 网络问题，关闭
                    if (jedis != null) {
                        jedis.close();
                        sleepStrategy.sleep();
                    }
                } catch (RuntimeException e) {
                    // 遇到task运行时异常，则记log （实际上FutureTask的run方法不会抛出任何异常）
                    logger.error("Error when task running in YedisExecutor", e);
                } catch (Error e) {
                    // 遇到task运行时异常，则记log（实际上FutureTask的run方法不会抛出任何异常）
                    logger.error("Error when task running in YedisExecutor", e);
                } catch (Throwable e) {
                    // 遇到task运行时异常，则记log（实际上FutureTask的run方法不会抛出任何异常）
                    logger.error("Error when task running in YedisExecutor", e);
                } finally {
                    ThreadLocalHolder.jedisHolder.remove();
                }
            }
        }

        private void checkJedis() {
            if (jedis == null || !jedis.isConnected()) {
                createJedis();
            }
        }

        private void createJedis() {
            jedis = new Jedis(config.getHost(), config.getPort(), config.getSocketTimeout());
            jedis.select(config.getDatabase());
            logger.info("Connected to " + config.getHost() + ":" + config.getPort());
            sleepStrategy.reset();
        }
    }

}
