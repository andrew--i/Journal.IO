package journal.io.api.operation;

import java.util.concurrent.*;

/**
 * Created by Andrew on 12.04.2017.
 */
public class WriteFuture implements Future<Boolean> {

    private final CountDownLatch latch;

    WriteFuture(CountDownLatch latch) {
        this.latch = latch != null ? latch : new CountDownLatch(0);
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cannot cancel this type of future!");
    }

    public boolean isCancelled() {
        throw new UnsupportedOperationException("Cannot cancel this type of future!");
    }

    public boolean isDone() {
        return latch.getCount() == 0;
    }

    public Boolean get() throws InterruptedException, ExecutionException {
        latch.await();
        return true;
    }

    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean success = latch.await(timeout, unit);
        return success;
    }
}
