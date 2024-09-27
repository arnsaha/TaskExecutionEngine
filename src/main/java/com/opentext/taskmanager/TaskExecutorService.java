package com.opentext.taskmanager;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TaskExecutorService implements TaskExecutor {
    private final ExecutorService executor;
    private final BlockingQueue<QueuedTask<?>> globalTaskQueue;
    /**
     * Group to Task mapping. So that all the tasks under the same group can be found together
     */
    private final Map<TaskGroup, Queue<QueuedTask<?>>> groupTaskQueues;
    private final Set<TaskGroup> executingGroups;
    private final Lock dispatcherLock = new ReentrantLock();
    private final AtomicBoolean isRunning = new AtomicBoolean(true); // To manage the worker thread status

    // This thred is responsible for runnting tasks from gloabal taskqueue.
    private final Thread workerThread;

    /**
     * Constructs a TaskExecutorService with the specified maximum concurrency.
     *
     * @param maxConcurrency the maximum number of concurrent tasks
     */
    public TaskExecutorService(int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be greater than 0");
        }
        this.executor = Executors.newFixedThreadPool(maxConcurrency);
        this.globalTaskQueue = new LinkedBlockingQueue<>();
        this.groupTaskQueues = new ConcurrentHashMap<>();
        this.executingGroups = ConcurrentHashMap.newKeySet();
        this.workerThread = new Thread(new WorkerThread<>());
        this.workerThread.start();
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        QueuedTask<T> queuedTask = new QueuedTask<>(task, future);
        globalTaskQueue.offer(queuedTask);
        return future;
    }

    /**
     *  Stopping execution of worker threads and shutting down the executor service .
     */
    public void shutdown() {
        try {
            isRunning.set(false);
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();
        System.out.println("Executor Shutdown complete");
    }

    /**
     * Helper class to encapsulate a task, its associated future, and submission time.
     *
     * @param <T> the result type returned by the task's computation
     */
    private static class QueuedTask<T> {
        private final Task<T> task;
        private final CompletableFuture<T> future;

        public QueuedTask(Task<T> task, CompletableFuture<T> future) {
            this.task = task;
            this.future = future;
        }

        public Task<T> getTask() {
            return task;
        }

        public void complete(T result) {
            future.complete(result);
        }

        public void completeExceptionally(Throwable e) {
            future.completeExceptionally(e);
        }
    }
    /**
     * Worker thread continuously processes the globalTaskQueue .
     */
    public class WorkerThread<T> implements Runnable{
        @Override
        public void run() {
            try {
                while (isRunning.get() || !globalTaskQueue.isEmpty()) {
                    QueuedTask<?> queuedTask = globalTaskQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (queuedTask != null) {
                        TaskGroup group = queuedTask.getTask().taskGroup();
                        dispatcherLock.lock();
                        try {
                            if (!executingGroups.contains(group)) {
                                executingGroups.add(group);
                                executeTask((QueuedTask<T>) queuedTask);
                                // After adding a tasks into the group  starting execution of the task
                            } else {
                                groupTaskQueues
                                        .computeIfAbsent(group, k -> new LinkedList<>())
                                        .offer(queuedTask);
                            }
                        }finally {
                            dispatcherLock.unlock();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void executeTask(QueuedTask<T> queuedTask) {
            executor.submit(() -> {
                try {
                    T result = queuedTask.getTask().taskAction().call();
                    queuedTask.complete(result);
                } catch (Exception e) {
                    queuedTask.completeExceptionally(e);
                } finally {
                    taskCompleted(queuedTask.getTask().taskGroup());
                }
            });
        }

        /**
         * All the tasks in the groups are performed one after another
         * When no tasks are present then remove the group from the map.
         *
         * @param group
         */
        private void taskCompleted(TaskGroup group) {
            dispatcherLock.lock();
            try {
                Queue<QueuedTask<?>> groupQueue = groupTaskQueues.get(group);
                if (groupQueue != null && !groupQueue.isEmpty()) {
                    QueuedTask<?> nextTask = groupQueue.poll();
                    if (nextTask != null) {
                        executeTask((QueuedTask<T>) nextTask);
                    }
                    if (groupQueue.isEmpty()) {
                        groupTaskQueues.remove(group);
                    }
                } else {
                    executingGroups.remove(group);
                }
            } finally {
                dispatcherLock.unlock();
            }
        }
    }
}
