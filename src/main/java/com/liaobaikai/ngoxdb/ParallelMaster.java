package com.liaobaikai.ngoxdb;

import com.liaobaikai.ngoxdb.core.listener.ParallelCallback;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @author baikai.liao
 * @Time 2021-03-18 17:56:32
 */
@Slf4j
public class ParallelMaster {

    private final ThreadPoolExecutor threadPoolExecutor;

    public ParallelMaster(int parallelWorkers, int threadPoolMaxThreads) {
        this(parallelWorkers, threadPoolMaxThreads, Integer.MAX_VALUE);
    }

    public ParallelMaster(int parallelWorkers, int threadPoolMaxThreads, int threadConcurrency) {

        log.info("parallelWorkers: {}", parallelWorkers);
        log.info("threadPoolMaxThreads: {}", threadPoolMaxThreads);
        log.info("threadConcurrency: {}", threadConcurrency);

        this.threadPoolExecutor = new ThreadPoolExecutor(
                parallelWorkers,
                threadPoolMaxThreads,
                0L,     // 超过THREAD_CACHE_SIZE的线程马上被终止
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(threadConcurrency),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static class ParallelRunner implements Runnable {

        private final int startIndex;
        private final CountDownLatch countDownLatch;
        private final Semaphore semaphore;
        private final ParallelCallback parallelCallback;

        public ParallelRunner(int startIndex,
                              CountDownLatch countDownLatch,
                              Semaphore semaphore,
                              ParallelCallback parallelCallback) {
            this.startIndex = startIndex;
            this.countDownLatch = countDownLatch;
            this.semaphore = semaphore;
            this.parallelCallback = parallelCallback;
        }

        @Override
        public void run() {
            try {
                semaphore.acquire();
                parallelCallback.callback(startIndex);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
                semaphore.release();
            }
        }
    }

    private static class ParallelCaller implements Callable<Object> {

        private final int startIndex;
        private final CountDownLatch countDownLatch;
        private final Semaphore semaphore;
        private final ParallelCallback parallelCallback;

        public ParallelCaller(int startIndex,
                              CountDownLatch countDownLatch,
                              Semaphore semaphore,
                              ParallelCallback parallelCallback) {
            this.startIndex = startIndex;
            this.countDownLatch = countDownLatch;
            this.semaphore = semaphore;
            this.parallelCallback = parallelCallback;
        }


        @Override
        public Object call() throws Exception {
            Object rsp;
            try {
                semaphore.acquire();
                parallelCallback.callback(startIndex);
                rsp = "SUCCESS";
            } catch (Exception e) {
                e.printStackTrace();
                rsp = e;
            } finally {
                countDownLatch.countDown();
                semaphore.release();
            }
            return rsp;
        }
    }

    /**
     * 并行执行
     *
     * @param loopCount          需要循环的总次数
     * @param onParallelListener 监听
     */
    public void parallelExecute(int loopCount, ParallelCallback onParallelListener) {

        CountDownLatch countDownLatch = new CountDownLatch(loopCount);
        Semaphore semaphore = new Semaphore(threadPoolExecutor.getMaximumPoolSize());

        // 创建线程
        for (int i = 0; i < loopCount; i++) {
            threadPoolExecutor.execute(new ParallelRunner(i, countDownLatch, semaphore, onParallelListener));
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 并行执行
     *
     * @param loopCount          需要循环的总次数
     * @param onParallelListener 监听
     */
    public void parallelSubmit(int loopCount, ParallelCallback onParallelListener) {

        CountDownLatch countDownLatch = new CountDownLatch(loopCount);
        Semaphore semaphore = new Semaphore(threadPoolExecutor.getMaximumPoolSize());

        // 创建线程
        for (int i = 0; i < loopCount; i++) {
            Future<Object> future = threadPoolExecutor.submit(new ParallelCaller(i, countDownLatch, semaphore, onParallelListener));
            try {
                Object result = future.get();
                if (result instanceof Exception) {
                    ((Exception) result).printStackTrace();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    // /**
    //  * 并行执行
    //  *
    //  * @param loopCount          需要循环的总次数
    //  * @param onParallelListener 监听
    //  */
    // public ParallelMaster parallel(long loopCount, OnParallelListener onParallelListener) {
    //
    //     int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
    //     int groupMinSize = (int) (loopCount / (long) maximumPoolSize);
    //     int lastSize = (int) (loopCount % (long) maximumPoolSize);
    //     int latchCount = groupMinSize == 0 ? lastSize : maximumPoolSize;
    //
    //     CountDownLatch countDownLatch = new CountDownLatch(latchCount);
    //
    //     // 创建线程
    //     for (int i = 0, startIndex, end = 0; i < maximumPoolSize; i++) {
    //
    //         startIndex = end;
    //         end = startIndex + groupMinSize;
    //         if (i < lastSize) {
    //             end += 1;
    //         } else if (i >= lastSize && groupMinSize == 0) {
    //             // 没有数据了
    //             break;
    //         }
    //
    //         int startX = startIndex;
    //         int finalEnd = end;
    //         Future<Object> future = threadPoolExecutor.submit(() -> {
    //             Object rsp;
    //             try {
    //                 for (int x = startX; x < finalEnd; x++) {
    //                     onParallelListener.onParallel(x, startX, finalEnd, loopCount);
    //                 }
    //                 rsp = "SUCCESS";
    //             } catch (Exception e) {
    //                 rsp = e;
    //                 e.printStackTrace();
    //             } finally {
    //                 countDownLatch.countDown();
    //             }
    //             return rsp;
    //         });
    //         try {
    //             Object result = future.get();
    //             if (result instanceof Exception) {
    //                 ((Exception) result).printStackTrace();
    //             }
    //         } catch (InterruptedException | ExecutionException e) {
    //             e.printStackTrace();
    //         }
    //
    //     }
    //
    //     try {
    //         countDownLatch.await();
    //     } catch (InterruptedException e) {
    //         e.printStackTrace();
    //     }
    //
    //     return this;
    // }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

    public void shutdown() {
        if (!this.threadPoolExecutor.isShutdown()) {
            System.out.println("shutdown...");
            this.threadPoolExecutor.shutdown();
        }
    }
}
