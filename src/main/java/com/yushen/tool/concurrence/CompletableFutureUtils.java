package com.yushen.tool.concurrence;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;
import java.util.function.Supplier;

public class CompletableFutureUtils {

    private static final long DEFAULT_TIMEOUT_VALUE = 10;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    private static ConcurrentMap<String, ExecutorService> poolCache = new ConcurrentHashMap<>();
    private static ConcurrentMap<String, ScheduledExecutorService> timeOutPoolCache = new ConcurrentHashMap<>();

    private static final Integer MAX_POOL_SIZE = 20;

    private static final ExecutorService DEFAULT_EXECUTOR = new ThreadPoolExecutor(32, 128,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(256),
            new ThreadFactoryBuilder()
                    .setNameFormat("CompletableFuture-executor-%d")
                    .build());

    private static final ScheduledExecutorService DEFAULT_TIMEOUT_EXECUTOR =
            Executors.newScheduledThreadPool(
                    32,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("CompletableFuture-timeout-executor-%d")
                            .build());

    /**
     * 注册一个专属线程池
     */
    private static void registePool(String serviceUniqueName) {

        if (poolCache.size() > MAX_POOL_SIZE || poolCache.containsKey(serviceUniqueName)) {
            return;
        }

        ExecutorService executor = new ThreadPoolExecutor(32, 128,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(256),
                new ThreadFactoryBuilder()
                        .setNameFormat(serviceUniqueName + "-CompletableFuture-executor-%d")
                        .build());

        ScheduledExecutorService timeOutExecutor = Executors.newScheduledThreadPool(
                32,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serviceUniqueName + "-CompletableFuture-timeout-executor-%d")
                        .build());

        poolCache.putIfAbsent(serviceUniqueName, executor);
        timeOutPoolCache.putIfAbsent(serviceUniqueName, timeOutExecutor);
    }

    /**
     * 无需返回结果
     */
    public static CompletableFuture<Void> run(Runnable runnable) {

        return run(runnable, DEFAULT_EXECUTOR, DEFAULT_TIMEOUT_VALUE, DEFAULT_TIME_UNIT,
                DEFAULT_TIMEOUT_EXECUTOR);
    }


    public static CompletableFuture<Void> run(String serviceUniqueName, Runnable runnable) {

        return run(serviceUniqueName, runnable, DEFAULT_TIMEOUT_VALUE);
    }


    public static CompletableFuture<Void> run(String serviceUniqueName, Runnable runnable,
                                              long timeoutValue) {

        registePool(serviceUniqueName);

        return run(runnable, getExecutor(serviceUniqueName), timeoutValue, DEFAULT_TIME_UNIT,
                getTimeOutExecutor(serviceUniqueName));
    }

    public static CompletableFuture<Void> run(Runnable runnable, ExecutorService executor,
                                              long timeoutValue,
                                              TimeUnit timeUnit, ScheduledExecutorService timeoutExecutor) {

        CompletableFuture<Void> cf = new CompletableFuture<>();

        Future<?> future = executor.submit(() -> {
            try {
                runnable.run();
                cf.complete(null);
            } catch (Throwable ex) {
                cf.completeExceptionally(ex);
            }
        });

        timeoutExecutor.schedule(() -> {
            if (!cf.isDone()) {
                cf.complete(null);
                future.cancel(true);
            }

        }, timeoutValue, timeUnit);

        return cf;
    }

    /**
     * 有返回结果
     */
    public static <T> CompletableFuture<T> async(Supplier<T> supplier, T defaultValue) {

        return async(supplier, DEFAULT_EXECUTOR, DEFAULT_TIMEOUT_VALUE, DEFAULT_TIME_UNIT,
                DEFAULT_TIMEOUT_EXECUTOR, defaultValue);
    }


    public static <T> CompletableFuture<T> async(String serviceUniqueName, Supplier<T> supplier, T defaultValue) {

        return async(serviceUniqueName, supplier,defaultValue, DEFAULT_TIMEOUT_VALUE);
    }


    public static <T> CompletableFuture<T> async(String serviceUniqueName, Supplier<T> supplier, T defaultValue,
                                                 long timeoutValue) {

        registePool(serviceUniqueName);

        return async(supplier, getExecutor(serviceUniqueName), timeoutValue, DEFAULT_TIME_UNIT,
                getTimeOutExecutor(serviceUniqueName), defaultValue);
    }

    public static <T> CompletableFuture<T> async(Supplier<T> supplier, ExecutorService executor,
                                                 long timeoutValue,
                                                 TimeUnit timeUnit, ScheduledExecutorService timeoutExecutor,
                                                 T defaultValue) {

        CompletableFuture<T> cf = new CompletableFuture<>();

        Future<?> future = executor.submit(() -> {
            try {
                cf.complete(supplier.get());
            } catch (Throwable ex) {
                cf.completeExceptionally(ex);
            }
        });

        timeoutExecutor.schedule(() -> {
            if (!cf.isDone()) {
                cf.complete(defaultValue);
                future.cancel(true);
            }

        }, timeoutValue, timeUnit);

        return cf;
    }

    private static ExecutorService getExecutor(String serviceUniqueName) {
        ExecutorService executor = poolCache.get(serviceUniqueName);
        if (executor != null) {
            return executor;
        }
        return DEFAULT_EXECUTOR;
    }

    private static ScheduledExecutorService getTimeOutExecutor(String serviceUniqueName) {
        ScheduledExecutorService executor = timeOutPoolCache.get(serviceUniqueName);
        if (executor != null) {
            return executor;
        }
        return DEFAULT_TIMEOUT_EXECUTOR;
    }
}
