package demo;

import rx.core.Observable;
import rx.disposable.Disposable;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

public class Demo {

    public static void main(String[] args) throws InterruptedException {

        sep("1. Observable.just + map + filter");
        Observable.just(1, 2, 3, 4, 5, 6)
                .filter(x -> x % 2 == 0)
                .map(x -> "even: " + x)
                .subscribe(
                        System.out::println,
                        Throwable::printStackTrace,
                        () -> System.out.println("  → complete"));

        sep("2. Observable.create with custom source");
        Observable.<String>create(e -> {
            for (String word : List.of("Hello", "Reactive", "World")) {
                e.onNext(word);
            }
            e.onComplete();
        })
        .map(String::toUpperCase)
        .subscribe(System.out::println);

        sep("3. flatMap – expand each item into a stream");
        Observable.just("Rx", "Java")
                .flatMap(s -> Observable.just(s + "-1", s + "-2", s + "-3"))
                .subscribe(System.out::println);

        sep("4. Error handling");
        Observable.just("10", "not-a-number", "30")
                .map(Integer::parseInt)     // will throw on "not-a-number"
                .subscribe(
                        item -> System.out.println("parsed: " + item),
                        err  -> System.out.println("caught: " + err.getClass().getSimpleName()
                                                   + " – " + err.getMessage()),
                        ()   -> System.out.println("completed (won't print after error)"));

        sep("5. Disposable – cancel subscription mid-stream");
        AtomicBoolean cancelFlag = new AtomicBoolean(false);
        Disposable[] ref = new Disposable[1];
        ref[0] = Observable.<Integer>create(e -> {
            for (int i = 1; i <= 10; i++) {
                if (cancelFlag.get()) {
                    System.out.println("  source noticed cancellation at i=" + i + ", stopping");
                    return;
                }
                e.onNext(i);
            }
        }).subscribe(x -> {
            System.out.println("received: " + x);
            if (x == 4) cancelFlag.set(true);
        });
        System.out.println("cancel flag: " + cancelFlag.get());

        sep("6. subscribeOn (IOThread) + observeOn (Single)");
        CountDownLatch latch = new CountDownLatch(1);
        Observable.<Integer>create(e -> {
            System.out.println("  source thread: " + Thread.currentThread().getName());
            e.onNext(100);
            e.onNext(200);
            e.onComplete();
        })
        .subscribeOn(Schedulers.io())
        .map(x -> x * 2)
        .observeOn(Schedulers.single())
        .subscribe(
                x -> System.out.println("  observer thread: "
                        + Thread.currentThread().getName() + " value=" + x),
                Throwable::printStackTrace,
                latch::countDown);

        latch.await(3, TimeUnit.SECONDS);

        sep("7. Computation scheduler – parallel work");
        int tasks = Runtime.getRuntime().availableProcessors();
        CountDownLatch cl = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; i++) {
            final int id = i;
            Schedulers.computation().execute(() -> {
                // CPU-bound simulation
                long sum = 0;
                for (int j = 0; j < 1_000_000; j++) sum += j;
                System.out.println("  task-" + id + " sum=" + sum
                        + " thread=" + Thread.currentThread().getName());
                cl.countDown();
            });
        }
        cl.await(5, TimeUnit.SECONDS);

        sep("8. Complex pipeline: IO source → computation transform → single observer");
        CountDownLatch done = new CountDownLatch(1);
        Observable.<Integer>create(e -> {
            // simulates slow I/O
            System.out.println("  [IO source] " + Thread.currentThread().getName());
            for (int i = 1; i <= 5; i++) {
                try { Thread.sleep(10); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                e.onNext(i);
            }
            e.onComplete();
        })
        .subscribeOn(Schedulers.io())
        .map(x -> {
            System.out.println("  [map] " + Thread.currentThread().getName());
            return x * x;
        })
        .observeOn(Schedulers.single())
        .subscribe(
                x  -> System.out.println("  [observer] " + Thread.currentThread().getName() + " x²=" + x),
                t  -> t.printStackTrace(),
                done::countDown);

        done.await(5, TimeUnit.SECONDS);
        System.out.println("\nDemo finished.");
    }

    private static void sep(String title) {
        System.out.println("\n══ " + title + " ══");
    }
}
