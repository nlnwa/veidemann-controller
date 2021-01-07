/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.controller;

import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.grpc.client.GrpcClientLimiterBuilder;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc.ConfigImplBase;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 *
 */
public class ApiBackpressureTest {
    private final String uniqueServerName = "in-process server for " + getClass();
    private ManagedChannel inProcessChannel;

    @BeforeEach
    public void beforeEachTest() throws DbException {
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).build();
    }

    @AfterEach
    public void afterEachTest() throws InterruptedException {
        inProcessChannel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    void requestThrottling() {
        AtomicInteger count = new AtomicInteger(0);
        try (ApiServerMock inProcessServer = new ApiServerMock(0, 50, null)) {
            ConfigGrpc.ConfigStub configClient = ConfigGrpc.newStub(inProcessChannel).withInterceptors(
                    new ConcurrencyLimitClientInterceptor(new GrpcClientLimiterBuilder().blockOnLimit(true).build())
            );

            for (int i = 0; i < 100; i++) {
                configClient.getConfigObject(ConfigRef.getDefaultInstance(), new StreamObserver<ConfigObject>() {
                    public void onNext(ConfigObject value) {
                        count.incrementAndGet();
                    }

                    public void onError(Throwable t) {
                    }

                    public void onCompleted() {
                    }
                });
            }
            assertThat(count.get()).isCloseTo(100, within(25));
        }
    }

    @Test
    void streamRequestBackPressure() throws InterruptedException {
        ListEventCallback callback = new ListEventCallback();
        AtomicInteger count = new AtomicInteger(0);

        try (ApiServerMock inProcessServer = new ApiServerMock(100, 0, callback)) {
            ConfigGrpc.ConfigBlockingStub configClient = ConfigGrpc.newBlockingStub(inProcessChannel);
            ListRequest request = ListRequest.getDefaultInstance();
            Iterator<ConfigObject> reply = configClient.listConfigObjects(request);
            new Thread(() -> {
                reply.forEachRemaining(o -> {
                    try {
                        Thread.sleep(10);
                        count.incrementAndGet();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }).start();
        }

        callback.waitForCompleted();
        assertThat(callback.onNextCount).isEqualTo(100);
        assertThat(count.get()).isCloseTo(100, within(5));
        assertThat(callback.exception).isEmpty();
    }

    @Test
    void streamRequestCancelledByClient() throws InterruptedException {
        ListEventCallback callback = new ListEventCallback();

        try (ApiServerMock inProcessServer = new ApiServerMock(100, 0, callback)) {
            ConfigGrpc.ConfigBlockingStub configClient = ConfigGrpc.newBlockingStub(inProcessChannel);

            ListRequest request = ListRequest.getDefaultInstance();
            Iterator<ConfigObject> reply = configClient.listConfigObjects(request);
            AtomicInteger count = new AtomicInteger(0);
            for (; reply.hasNext(); reply.next()) {
                if (count.getAndIncrement() > 5) {
                    ManagedChannel c = (ManagedChannel) configClient.getChannel();
                    c.shutdownNow();
                    c.awaitTermination(10, TimeUnit.SECONDS);
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        callback.waitForCompleted();
        assertThat(callback.onNextCount).isGreaterThanOrEqualTo(5).isLessThan(100);
        assertThat(callback.exception).hasValueSatisfying(e -> {
            assertThat(e).isInstanceOf(StatusRuntimeException.class);
            assertThat(((StatusRuntimeException) e).getStatus()).isEqualTo(Status.ABORTED);
        });
    }

    class RepeatingChangeFeed<T> implements ChangeFeed<T> {
        int count;
        final T value;

        public RepeatingChangeFeed(int count, T value) {
            this.count = count;
            this.value = value;
        }

        @Override
        public Stream<T> stream() {
            Iterator<T> it = new Iterator<T>() {
                int c = count;

                @Override
                public boolean hasNext() {
                    return c > 0;
                }

                @Override
                public T next() {
                    c--;
                    return value;
                }
            };

            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
        }

        @Override
        public void close() {
        }
    }

    class ListEventCallback {
        Lock lock = new ReentrantLock();
        Condition notDone = lock.newCondition();
        int onNextCount;
        Optional<Exception> exception;

        public void completed(int onNextCount, Optional<Exception> exception) {
            this.onNextCount = onNextCount;
            this.exception = exception;

            lock.lock();
            try {
                notDone.signal();
            } finally {
                lock.unlock();
            }
        }

        void waitForCompleted() {
            lock.lock();
            try {
                while (exception == null) {
                    notDone.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    class ApiServerMock implements AutoCloseable {
        private Server server;
        private ExecutorService executor;

        public ApiServerMock(int listResponseCount, long getObjectDelay, ListEventCallback cb) {
            executor = Executors.newFixedThreadPool(8);
            server = InProcessServerBuilder.forName(uniqueServerName).executor(executor)
                    .addService(new ConfigImplBase() {
                        @Override
                        public void listConfigObjects(ListRequest request, StreamObserver<ConfigObject> observer) {
                            StreamObserver<ConfigObject> responseObserver = new BlockingStreamObserver<>(observer);
                            AtomicInteger onNextCount = new AtomicInteger(0);
                            new Thread(() -> {
                                try (ChangeFeed<ConfigObject> c = new RepeatingChangeFeed<>(listResponseCount, ConfigObject.getDefaultInstance());) {
                                    c.stream().forEach(o -> {
                                        responseObserver.onNext(o);
                                        onNextCount.incrementAndGet();
                                    });
                                    responseObserver.onCompleted();
                                    cb.completed(onNextCount.get(), Optional.empty());
                                } catch (StatusRuntimeException e) {
                                    responseObserver.onError(e);
                                    cb.completed(onNextCount.get(), Optional.of(e));
                                } catch (Exception ex) {
                                    Status status = Status.UNKNOWN.withDescription(ex.toString());
                                    responseObserver.onError(status.asException());
                                    cb.completed(onNextCount.get(), Optional.of(ex));
                                }
                            }).start();
                        }

                        @Override
                        public void getConfigObject(ConfigRef request, StreamObserver<ConfigObject> responseObserver) {
                            try {
                                Thread.sleep(getObjectDelay);
                                handleGet(() -> ConfigObject.getDefaultInstance(), responseObserver);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    })
                    .build();
            try {
                server.start();
            } catch (IOException ex) {
                close();
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        public void close() {
            if (server != null) {
                try {
                    server.shutdown().awaitTermination(5L, TimeUnit.SECONDS);
                    executor.shutdownNow();
                    executor.awaitTermination(5L, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    server.shutdown();
                }
            }
        }
    }
}
