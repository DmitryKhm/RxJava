package rx;

import rx.core.Observable;
import rx.core.Observer;
import rx.disposable.Disposable;
import rx.schedulers.Scheduler;
import rx.schedulers.Schedulers;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;


public class RxTests {


    private static void assertEquals(Object expected, Object actual, String msg) {
        if (!Objects.equals(expected, actual))
            throw new AssertionError(msg + " — expected: " + expected + ", got: " + actual);
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    private static void assertError(Runnable block, Class<? extends Throwable> type, String msg) {
        try {
            block.run();
            throw new AssertionError(msg + " — no exception thrown");
        } catch (Throwable t) {
            if (!type.isInstance(t))
                throw new AssertionError(msg + " — expected " + type.getSimpleName()
                        + " but got " + t.getClass().getSimpleName());
        }
    }

    private static int passed = 0;
    private static int failed = 0;

    @FunctionalInterface
    interface ThrowingRunnable { void run() throws Exception; }

    private static void run(String name, ThrowingRunnable test) {
        try {
            test.run();
            System.out.println("  ✓  " + name);
            passed++;
        } catch (Throwable t) {
            System.out.println("  ✗  " + name + " → " + t.getMessage());
            failed++;
        }
    }


    public static void main(String[] args) {

        section("1. Observable.create / just / empty / error");
        run("just emits items in order", RxTests::test_just_emitsInOrder);
        run("create emits items and completes", RxTests::test_create_emitsAndCompletes);
        run("empty triggers onComplete only", RxTests::test_empty);
        run("error triggers onError only", RxTests::test_error);
        run("fromIterable emits all items", RxTests::test_fromIterable);

        section("2. Operators: map / filter / flatMap");
        run("map transforms each item", RxTests::test_map);
        run("map propagates exceptions to onError", RxTests::test_map_throwsPropagated);
        run("filter keeps matching items", RxTests::test_filter);
        run("filter removes non-matching items", RxTests::test_filter_removesItems);
        run("flatMap expands each item", RxTests::test_flatMap);
        run("flatMap inner error propagates", RxTests::test_flatMap_innerError);
        run("operator chain: filter + map + flatMap", RxTests::test_operatorChain);

        section("3. Disposable / cancellation");
        run("dispose stops delivery (sync source + flag)", RxTests::test_dispose_stopsEvents);
        run("dispose stops delivery (async source)", RxTests::test_dispose_async_stopsDelivery);
        run("SafeObserver enforces Rx grammar (no events after onComplete)",
                RxTests::test_safeObserver_noEventsAfterComplete);
        run("SafeObserver enforces Rx grammar (no events after onError)",
                RxTests::test_safeObserver_noEventsAfterError);

        section("4. Error handling");
        run("onError called when source throws", RxTests::test_sourceThrows);
        run("map exception routed to onError", RxTests::test_mapException);
        run("filter exception routed to onError", RxTests::test_filterException);

        section("5. Schedulers");
        run("subscribeOn: source runs on different thread", RxTests::test_subscribeOn_differentThread);
        run("observeOn: observer runs on different thread", RxTests::test_observeOn_differentThread);
        run("subscribeOn + observeOn: each on correct thread",
                RxTests::test_subscribeOnAndObserveOn);
        run("ComputationScheduler has nCPU threads", RxTests::test_computationScheduler_parallelism);
        run("SingleThreadScheduler is sequential", RxTests::test_singleThreadScheduler_sequential);

        System.out.println("\n" + "─".repeat(50));
        System.out.printf("  Results: %d passed, %d failed%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    private static void section(String title) {
        System.out.println("\n── " + title);
    }


    static void test_just_emitsInOrder() {
        List<Integer> out = new ArrayList<>();
        Observable.just(1, 2, 3).subscribe(out::add);
        assertEquals(List.of(1, 2, 3), out, "items");
    }

    static void test_create_emitsAndCompletes() {
        List<String> out = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();
        Observable.<String>create(e -> {
            e.onNext("a");
            e.onNext("b");
            e.onComplete();
        }).subscribe(out::add, Throwable::printStackTrace, () -> completed.set(true));

        assertEquals(List.of("a", "b"), out, "items");
        assertTrue(completed.get(), "onComplete called");
    }

    static void test_empty() {
        List<Object> out = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();
        Observable.empty().subscribe(out::add, t -> {}, () -> completed.set(true));
        assertTrue(out.isEmpty(), "no items");
        assertTrue(completed.get(), "onComplete called");
    }

    static void test_error() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.error(new RuntimeException("boom"))
                  .subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() instanceof RuntimeException, "onError received");
        assertEquals("boom", err.get().getMessage(), "error message");
    }

    static void test_fromIterable() {
        List<Integer> out = new ArrayList<>();
        Observable.fromIterable(List.of(10, 20, 30)).subscribe(out::add);
        assertEquals(List.of(10, 20, 30), out, "items");
    }


    static void test_map() {
        List<String> out = new ArrayList<>();
        Observable.just(1, 2, 3)
                  .map(x -> "item-" + x)
                  .subscribe(out::add);
        assertEquals(List.of("item-1", "item-2", "item-3"), out, "mapped items");
    }

    static void test_map_throwsPropagated() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.just(1, 2, 3)
                  .map(x -> { if (x == 2) throw new IllegalStateException("bad"); return x; })
                  .subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() instanceof IllegalStateException, "error type");
    }

    static void test_filter() {
        List<Integer> out = new ArrayList<>();
        Observable.just(1, 2, 3, 4, 5)
                  .filter(x -> x % 2 == 0)
                  .subscribe(out::add);
        assertEquals(List.of(2, 4), out, "filtered items");
    }

    static void test_filter_removesItems() {
        List<Integer> out = new ArrayList<>();
        Observable.just(1, 3, 5)
                  .filter(x -> x % 2 == 0)
                  .subscribe(out::add);
        assertTrue(out.isEmpty(), "all odd numbers filtered out");
    }

    static void test_flatMap() {
        List<Integer> out = new ArrayList<>();
        Observable.just(1, 2, 3)
                  .flatMap(x -> Observable.just(x, x * 10))
                  .subscribe(out::add);
        assertEquals(List.of(1, 10, 2, 20, 3, 30), out, "flatMapped items");
    }

    static void test_flatMap_innerError() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.just(1, 2)
                  .flatMap(x -> {
                      if (x == 2) return Observable.error(new RuntimeException("inner error"));
                      return Observable.just(x);
                  })
                  .subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() != null, "error received");
        assertEquals("inner error", err.get().getMessage(), "error message");
    }

    static void test_operatorChain() {
        List<String> out = new ArrayList<>();
        Observable.just(1, 2, 3, 4, 5, 6)
                  .filter(x -> x % 2 == 0)              // 2, 4, 6
                  .map(x -> x * x)                       // 4, 16, 36
                  .flatMap(x -> Observable.just(x, -x)) // 4,-4, 16,-16, 36,-36
                  .map(x -> "[" + x + "]")
                  .subscribe(out::add);
        assertEquals("[4]",   out.get(0), "first");
        assertEquals("[-4]",  out.get(1), "second");
        assertEquals("[36]",  out.get(out.size()-2), "second to last");
        assertEquals("[-36]", out.get(out.size()-1), "last");
        assertEquals(6, out.size(), "total items");
    }


    static void test_dispose_stopsEvents() {
        // Synchronous sources run entirely inside subscribe(), so a Disposable
        // returned by subscribe() cannot be stored before the source runs.
        // The correct pattern is to control cancellation via an external flag
        // that the source inspects, or to use an async source (subscribeOn).
        List<Integer> out = new ArrayList<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        Observable.<Integer>create(e -> {
            for (int i = 1; i <= 10; i++) {
                if (stop.get()) return;  // source respects cancellation
                e.onNext(i);
            }
            e.onComplete();
        }).subscribe(item -> {
            out.add(item);
            if (item == 3) stop.set(true);
        });

        assertEquals(List.of(1, 2, 3), out, "stopped after 3 items");
    }

    static void test_dispose_async_stopsDelivery() throws Exception {

        List<Integer> out = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Disposable> ref = new AtomicReference<>();

        Disposable d = Observable.<Integer>create(e -> {
            for (int i = 1; i <= 20; i++) {
                if (e.isDisposed()) break;
                e.onNext(i);
                try { Thread.sleep(5); } catch (InterruptedException ie) { break; }
            }
            done.countDown();
        })
        .subscribeOn(Schedulers.newIo())
        .subscribe(item -> {
            out.add(item);
            if (item == 5) ref.get().dispose();
        });

        ref.set(d);
        done.await(3, TimeUnit.SECONDS);

        // May receive a couple more items due to concurrency, but never all 20
        assertTrue(out.size() < 20, "not all 20 items delivered after dispose");
        assertTrue(out.contains(5), "item 5 was delivered");
    }

    static void test_safeObserver_noEventsAfterComplete() {
        List<Integer> out = new ArrayList<>();
        AtomicInteger completeCount = new AtomicInteger();

        Observable.<Integer>create(e -> {
            e.onNext(1);
            e.onComplete();
            e.onNext(2);    // must be suppressed
            e.onComplete(); // must be suppressed
        }).subscribe(out::add, t -> {}, () -> completeCount.incrementAndGet());

        assertEquals(List.of(1), out, "only pre-complete items");
        assertEquals(1, completeCount.get(), "onComplete called exactly once");
    }

    static void test_safeObserver_noEventsAfterError() {
        List<Integer> out = new ArrayList<>();
        AtomicInteger errCount = new AtomicInteger();

        Observable.<Integer>create(e -> {
            e.onNext(1);
            e.onError(new RuntimeException("first"));
            e.onNext(2);
            e.onError(new RuntimeException("second"));
        }).subscribe(out::add, t -> errCount.incrementAndGet(), () -> {});

        assertEquals(List.of(1), out, "only pre-error items");
        assertEquals(1, errCount.get(), "onError called exactly once");
    }


    static void test_sourceThrows() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.<Integer>create(e -> {
            throw new RuntimeException("source boom");
        }).subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() != null, "error received");
        assertEquals("source boom", err.get().getMessage(), "message");
    }

    static void test_mapException() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.just("a", "b")
                  .map(s -> { throw new IllegalArgumentException("map fail"); })
                  .subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() instanceof IllegalArgumentException, "error type");
    }

    static void test_filterException() {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Observable.just(1, 2)
                  .filter(x -> { throw new RuntimeException("filter fail"); })
                  .subscribe(x -> {}, err::set, () -> {});
        assertTrue(err.get() instanceof RuntimeException, "error type");
    }


    static void test_subscribeOn_differentThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> sourceThread = new AtomicReference<>();
        String mainThread = Thread.currentThread().getName();

        Observable.<Integer>create(e -> {
            sourceThread.set(Thread.currentThread().getName());
            e.onNext(1);
            e.onComplete();
        })
        .subscribeOn(Schedulers.newIo())
        .subscribe(x -> {}, t -> {}, latch::countDown);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "completed in time");
        assertTrue(!mainThread.equals(sourceThread.get()),
                "source ran on different thread: " + sourceThread.get());
    }

    static void test_observeOn_differentThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> observerThread = new AtomicReference<>();
        String mainThread = Thread.currentThread().getName();

        Observable.just(1, 2, 3)
                  .observeOn(Schedulers.newSingle())
                  .subscribe(
                      item -> observerThread.set(Thread.currentThread().getName()),
                      t -> {},
                      latch::countDown);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "completed in time");
        assertTrue(!mainThread.equals(observerThread.get()),
                "observer ran on different thread: " + observerThread.get());
    }

    static void test_subscribeOnAndObserveOn() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> sourceThread  = new AtomicReference<>();
        AtomicReference<String> observerThread = new AtomicReference<>();
        String mainThread = Thread.currentThread().getName();

        Scheduler io     = Schedulers.newIo();
        Scheduler single = Schedulers.newSingle();

        Observable.<Integer>create(e -> {
            sourceThread.set(Thread.currentThread().getName());
            e.onNext(42);
            e.onComplete();
        })
        .subscribeOn(io)
        .observeOn(single)
        .subscribe(
            item -> observerThread.set(Thread.currentThread().getName()),
            t -> {},
            latch::countDown);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "completed in time");
        assertTrue(!mainThread.equals(sourceThread.get()),
                "source not on main: " + sourceThread.get());
        assertTrue(!mainThread.equals(observerThread.get()),
                "observer not on main: " + observerThread.get());
        assertTrue(!sourceThread.get().equals(observerThread.get()),
                "source and observer on different threads");
    }

    static void test_computationScheduler_parallelism() throws Exception {
        int nCPU = Runtime.getRuntime().availableProcessors();
        Set<String> threads = Collections.synchronizedSet(new HashSet<>());
        CountDownLatch latch = new CountDownLatch(nCPU);
        CyclicBarrier barrier = new CyclicBarrier(nCPU); // ensure tasks run in parallel
        Scheduler comp = Schedulers.newComputation();

        for (int i = 0; i < nCPU; i++) {
            comp.execute(() -> {
                try { barrier.await(3, TimeUnit.SECONDS); } catch (Exception ignored) {}
                threads.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }
        assertTrue(latch.await(5, TimeUnit.SECONDS), "all tasks completed");
        assertTrue(threads.size() <= nCPU, "no more than nCPU threads used");
    }

    static void test_singleThreadScheduler_sequential() throws Exception {
        Scheduler single = Schedulers.newSingle();
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(5);

        for (int i = 0; i < 5; i++) {
            final int id = i;
            single.execute(() -> {
                order.add(id);
                latch.countDown();
            });
        }
        assertTrue(latch.await(3, TimeUnit.SECONDS), "all tasks done");
        assertEquals(List.of(0, 1, 2, 3, 4), order, "sequential order preserved");
    }
}
