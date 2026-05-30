package rx.disposable;

import java.util.concurrent.atomic.AtomicBoolean;


public interface Disposable {

    void dispose();

    boolean isDisposed();


    static Disposable disposed() {
        return new AtomicDisposable(true);
    }

    static AtomicDisposable create() {
        return new AtomicDisposable(false);
    }
}
