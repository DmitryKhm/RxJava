package rx.schedulers;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;


class IOThreadScheduler implements Scheduler {

    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "rx-io-" + System.nanoTime());
        t.setDaemon(true);
        return t;
    });

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }
}



class ComputationScheduler implements Scheduler {

    private final int parallelism = Runtime.getRuntime().availableProcessors();
    private final ExecutorService executor = Executors.newFixedThreadPool(parallelism, r -> {
        Thread t = new Thread(r, "rx-computation-" + System.nanoTime());
        t.setDaemon(true);
        return t;
    });

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }
}



class SingleThreadScheduler implements Scheduler {

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "rx-single");
        t.setDaemon(true);
        return t;
    });

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }
}



public final class Schedulers {

    private static final Scheduler IO          = new IOThreadScheduler();
    private static final Scheduler COMPUTATION = new ComputationScheduler();
    private static final Scheduler SINGLE      = new SingleThreadScheduler();

    private Schedulers() {}

    public static Scheduler io()          { return IO; }

    /** Fixed pool (nCPU threads) — for CPU-bound tasks. */
    public static Scheduler computation() { return COMPUTATION; }

    /** Single background thread — for serialised/ordered execution. */
    public static Scheduler single()      { return SINGLE; }

    /**
     * Creates a fresh {@link Scheduler} backed by a new thread pool.
     * Useful in tests to avoid shared state.
     */
    public static Scheduler newIo()          { return new IOThreadScheduler(); }
    public static Scheduler newComputation() { return new ComputationScheduler(); }
    public static Scheduler newSingle()      { return new SingleThreadScheduler(); }
}
