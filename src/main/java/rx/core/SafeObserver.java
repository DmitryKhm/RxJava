package rx.core;

import rx.disposable.AtomicDisposable;
import rx.disposable.Disposable;

import java.util.concurrent.atomic.AtomicBoolean;


public final class SafeObserver<T> implements Observer<T>, Disposable {

    private final Observer<T> downstream;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicDisposable disposable = Disposable.create();

    public SafeObserver(Observer<T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onNext(T item) {
        if (terminated.get() || disposable.isDisposed()) return;
        try {
            downstream.onNext(item);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!terminated.compareAndSet(false, true)) return;
        try {
            downstream.onError(t);
        } catch (Throwable inner) {
            // Cannot deliver error – last resort
            Thread.currentThread().getUncaughtExceptionHandler()
                    .uncaughtException(Thread.currentThread(), inner);
        }
    }

    @Override
    public void onComplete() {
        if (!terminated.compareAndSet(false, true)) return;
        try {
            downstream.onComplete();
        } catch (Throwable t) {
            Thread.currentThread().getUncaughtExceptionHandler()
                    .uncaughtException(Thread.currentThread(), t);
        }
    }

    @Override
    public void dispose() {
        disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }
}
