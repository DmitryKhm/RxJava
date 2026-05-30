package rx.schedulers;


public interface Scheduler {



    void execute(Runnable task);


    default void shutdown() {}
}
