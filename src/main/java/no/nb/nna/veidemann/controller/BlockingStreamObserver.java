package no.nb.nna.veidemann.controller;

import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingStreamObserver<T> implements StreamObserver<T> {
    final StreamObserver<T> delegate;
    final Lock lock = new ReentrantLock();
    final Condition notReady = lock.newCondition();

    public BlockingStreamObserver(StreamObserver<T> delegate) {
        this.delegate = delegate;

        final Runnable notifyAll = () -> {
            lock.lock();
            try {
                notReady.signal();
            } finally {
                lock.unlock();
            }
        };
        if (delegate instanceof CallStreamObserver) {
            ((CallStreamObserver<T>) this.delegate).setOnReadyHandler(notifyAll);
        }
        if (delegate instanceof ServerCallStreamObserver) {
            ((ServerCallStreamObserver<T>) delegate).setOnCancelHandler(notifyAll);
        }
    }

    @Override
    public void onNext(T value) {
        if (delegate instanceof CallStreamObserver) {
            lock.lock();
            try {
                while (!((CallStreamObserver<T>) this.delegate).isReady()) {
                    notReady.await(100, TimeUnit.MILLISECONDS);
                    if (delegate instanceof ServerCallStreamObserver && ((ServerCallStreamObserver<T>) delegate).isCancelled()) {
                        throw Status.ABORTED.asRuntimeException();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
        }
        delegate.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }
}
