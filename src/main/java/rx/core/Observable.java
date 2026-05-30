package rx.core;

import rx.disposable.Disposable;
import rx.schedulers.Scheduler;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents a cold, push-based stream of items.
 *
 * <h2>Cold vs Hot</h2>
 * A <em>cold</em> Observable starts producing items only when a subscriber
 * subscribes, and each subscriber gets its own independent execution.
 * This simplifies reasoning about side-effects and is the default here.
 *
 * <h2>Operator chaining</h2>
 * Every operator returns a <em>new</em> Observable that wraps the previous one.
 * The resulting chain is assembled lazily; nothing runs until
 * {@link #subscribe(Observer)} is called.
 *
 * @param <T> the type of items emitted
 */
public final class Observable<T> {

    /**
     * The "source" lambda: given a {@link SafeObserver}, it pushes events.
     * This is the only mutable (well, functional) state of an Observable.
     */
    private final OnSubscribe<T> source;

    /** Functional interface for the source lambda stored in each Observable. */
    @FunctionalInterface
    public interface OnSubscribe<T> {
        void subscribe(SafeObserver<T> observer);
    }


    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }


    /**
     * Creates an Observable from user-supplied push logic.
     *
     * <pre>{@code
     * Observable.create(emitter -> {
     *     emitter.onNext(1);
     *     emitter.onNext(2);
     *     emitter.onComplete();
     * });
     * }</pre>
     */
    public static <T> Observable<T> create(java.util.function.Consumer<Emitter<T>> sourceAction) {
        return new Observable<>(safeObserver -> {
            Emitter<T> emitter = new Emitter<>() {
                @Override public void onNext(T item)    { safeObserver.onNext(item); }
                @Override public void onError(Throwable t) { safeObserver.onError(t); }
                @Override public void onComplete()      { safeObserver.onComplete(); }
                @Override public boolean isDisposed()   { return safeObserver.isDisposed(); }
            };
            try {
                sourceAction.accept(emitter);
            } catch (Throwable t) {
                safeObserver.onError(t);
            }
        });
    }


    @SafeVarargs
    public static <T> Observable<T> just(T... items) {
        return create(emitter -> {
            for (T item : items) {
                if (emitter.isDisposed()) return;
                emitter.onNext(item);
            }
            emitter.onComplete();
        });
    }


    public static <T> Observable<T> fromIterable(Iterable<T> source) {
        return create(emitter -> {
            for (T item : source) {
                if (emitter.isDisposed()) return;
                emitter.onNext(item);
            }
            emitter.onComplete();
        });
    }


    public static <T> Observable<T> empty() {
        return create(emitter -> emitter.onComplete());
    }

    public static <T> Observable<T> error(Throwable t) {
        return create(emitter -> emitter.onError(t));
    }


    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new Observable<>(downstream ->
            this.subscribe(new Observer<T>() {
                @Override public void onNext(T item) {
                    R result;
                    try {
                        result = mapper.apply(item);
                    } catch (Throwable t) {
                        downstream.onError(t);
                        return;
                    }
                    downstream.onNext(result);
                }
                @Override public void onError(Throwable t)  { downstream.onError(t); }
                @Override public void onComplete()           { downstream.onComplete(); }
            })
        );
    }


    public Observable<T> filter(Predicate<? super T> predicate) {
        return new Observable<>(downstream ->
            this.subscribe(new Observer<T>() {
                @Override public void onNext(T item) {
                    boolean passes;
                    try {
                        passes = predicate.test(item);
                    } catch (Throwable t) {
                        downstream.onError(t);
                        return;
                    }
                    if (passes) downstream.onNext(item);
                }
                @Override public void onError(Throwable t)  { downstream.onError(t); }
                @Override public void onComplete()           { downstream.onComplete(); }
            })
        );
    }


    @SuppressWarnings("unchecked")
    public <R> Observable<R> flatMap(Function<? super T, ? extends Observable<? extends R>> mapper) {
        return new Observable<>(downstream ->
            this.subscribe(new Observer<T>() {
                @Override public void onNext(T item) {
                    Observable<R> inner;
                    try {
                        // Safe cast: we only read R values from the inner Observable
                        inner = (Observable<R>) mapper.apply(item);
                    } catch (Throwable t) {
                        downstream.onError(t);
                        return;
                    }
                    // Subscribe synchronously to inner and forward its events
                    inner.subscribe(new Observer<R>() {
                        @Override public void onNext(R r)        { downstream.onNext(r); }
                        @Override public void onError(Throwable t) { downstream.onError(t); }
                        @Override public void onComplete()       { /* inner done; outer continues */ }
                    });
                }
                @Override public void onError(Throwable t)  { downstream.onError(t); }
                @Override public void onComplete()           { downstream.onComplete(); }
            })
        );
    }




    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(downstream ->
            scheduler.execute(() -> this.source.subscribe(downstream))
        );
    }


    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(downstream ->
            this.subscribe(new Observer<T>() {
                @Override public void onNext(T item) {
                    scheduler.execute(() -> downstream.onNext(item));
                }
                @Override public void onError(Throwable t) {
                    scheduler.execute(() -> downstream.onError(t));
                }
                @Override public void onComplete() {
                    scheduler.execute(downstream::onComplete);
                }
            })
        );
    }


    public Disposable subscribe(Observer<T> observer) {
        SafeObserver<T> safe = new SafeObserver<>(observer);
        try {
            source.subscribe(safe);
        } catch (Throwable t) {
            safe.onError(t);
        }
        return safe;
    }


    public Disposable subscribe(
            java.util.function.Consumer<? super T> onNext,
            java.util.function.Consumer<Throwable> onError,
            Runnable onComplete) {
        return subscribe(new Observer<T>() {
            @Override public void onNext(T item)       { onNext.accept(item); }
            @Override public void onError(Throwable t) { onError.accept(t); }
            @Override public void onComplete()          { onComplete.run(); }
        });
    }


    public Disposable subscribe(java.util.function.Consumer<? super T> onNext) {
        return subscribe(onNext,
                t -> Thread.currentThread().getUncaughtExceptionHandler()
                           .uncaughtException(Thread.currentThread(), t),
                () -> {});
    }
}
