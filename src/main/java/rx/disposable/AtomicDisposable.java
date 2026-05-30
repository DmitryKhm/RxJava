package rx.disposable;

import java.util.concurrent.atomic.AtomicBoolean;


public final class AtomicDisposable implements Disposable {

    private final AtomicBoolean disposed;

    AtomicDisposable(boolean initialState) {
        this.disposed = new AtomicBoolean(initialState);
    }

    @Override
    public void dispose() {
        disposed.set(true);
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    @Override
    public String toString() {
        return "AtomicDisposable{disposed=" + disposed.get() + "}";
    }
}
