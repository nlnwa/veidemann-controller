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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.controller.v1.ControllerGrpc;
import no.nb.nna.veidemann.api.controller.v1.CrawlerStatus;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlReply;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc.FrontierImplBase;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ApiConcurrencyTest {

    @Test
    void concurrentCalls() throws DbException, InterruptedException {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        ConfigAdapter configAdapterMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(configAdapterMock);
        ExecutionsAdapter executionsAdapterMock = mock(ExecutionsAdapter.class);
        when(dbProviderMock.getExecutionsAdapter()).thenReturn(executionsAdapterMock);

        String controllerName = "controller";
        String frontierName = "frontier";
        ManagedChannel controllerChannel = InProcessChannelBuilder.forName(controllerName).build();
        ManagedChannelBuilder frontierChannel = InProcessChannelBuilder.forName(frontierName);
        ExecutorService threadPool = Executors.newCachedThreadPool();
        ServerBuilder controllerServerBuilder = InProcessServerBuilder.forName(controllerName).executor(threadPool);
        ServerBuilder frontierServerBuilder = InProcessServerBuilder.forName(frontierName).executor(threadPool);

        Settings settings = new Settings();
        settings.setSkipAuthentication(true);

        try (DbService db = DbService.configure(dbProviderMock);
             ControllerApiServer controller = new ControllerApiServer(settings, controllerServerBuilder, null, null);
             FrontierMock frontier = new FrontierMock(frontierServerBuilder);
             FrontierClient frontierClient = new FrontierClient(frontierChannel, "url")
        ) {

            ConfigObject entity = newConfObj(Kind.crawlEntity, "en1").build();
            ConfigRef entityRef = ApiTools.refForConfig(entity);
            when(configAdapterMock.getConfigObject(entityRef))
                    .thenReturn(entity);

            ConfigObject seed = newConfObj(Kind.seed, "seed1").build();
            ConfigRef seedRef = ApiTools.refForConfig(seed);
            when(configAdapterMock.getConfigObject(seedRef))
                    .thenReturn(seed);

            ConfigObject scopescript = newConfObj(Kind.browserScript, "scp").build();
            ConfigRef scopescriptRef = ApiTools.refForConfig(scopescript);
            when(configAdapterMock.getConfigObject(scopescriptRef))
                    .thenReturn(scopescript);

            ConfigObject browserConfig = newConfObj(Kind.browserConfig, "bc")
                    .build();
            ConfigRef browserConfigRef = ApiTools.refForConfig(browserConfig);
            when(configAdapterMock.getConfigObject(browserConfigRef))
                    .thenReturn(browserConfig);

            ConfigObject crawlConfig = newConfObj(Kind.crawlConfig, "cc")
                    .setCrawlConfig(CrawlConfig.newBuilder().setBrowserConfigRef(browserConfigRef))
                    .build();
            ConfigRef crawlConfigRef = ApiTools.refForConfig(crawlConfig);
            when(configAdapterMock.getConfigObject(crawlConfigRef))
                    .thenReturn(crawlConfig);

            ConfigObject.Builder jobConfigB = newConfObj(Kind.crawlJob, "job1");
            jobConfigB.getCrawlJobBuilder()
                    .setCrawlConfigRef(crawlConfigRef)
                    .setScopeScriptRef(scopescriptRef);
            ConfigObject jobConfig1 = jobConfigB.build();
            ConfigRef jobConfig1Ref = ApiTools.refForConfig(jobConfig1);
            when(configAdapterMock.getConfigObject(jobConfig1Ref))
                    .thenReturn(jobConfig1);

            when(executionsAdapterMock.listJobExecutionStatus(any()))
                    .thenReturn(new ArrayChangeFeed<>());

            when(configAdapterMock.listConfigObjects(any()))
                    .thenReturn(new RepeatingChangeFeed<>(10000, seed));

            when(executionsAdapterMock.createJobExecutionStatus(any()))
                    .thenReturn(JobExecutionStatus.newBuilder().setId("jeid1").build());

            when(executionsAdapterMock.getDesiredPausedState()).thenReturn(false);

            JobExecutionStartedListener jobStarted = new JobExecutionStartedListener();
            controller.addJobExecutionListener(jobStarted);
            controller.start();
            ControllerGrpc.ControllerFutureStub controllerClient = ControllerGrpc.newFutureStub(controllerChannel);

            ListenableFuture<RunCrawlReply> reply = controllerClient.runCrawl(RunCrawlRequest.newBuilder().setJobId(jobConfig1.getId()).build());

            // Execute status request
            ListenableFuture<CrawlerStatus> statusReply = controllerClient.status(Empty.getDefaultInstance());

            // Record time when status returns
            AtomicLong statusReplyTime = new AtomicLong();
            statusReply.addListener(() -> statusReplyTime.set(System.currentTimeMillis()), ForkJoinPool.commonPool());

            // Wait for job to be started
            assertThat(jobStarted.waitForStarted(5, TimeUnit.SECONDS)).isTrue();
            long time = System.currentTimeMillis();

            // Ensure that status request was handled while job was starting
            assertThat(statusReplyTime.get()).isLessThan(time).isNotCloseTo(time, Offset.offset(50L));
        }
    }

    class FrontierMock implements AutoCloseable {
        private Server server;
        private ExecutorService executor;

        public FrontierMock(ServerBuilder frontierServerBuilder) {
            executor = Executors.newCachedThreadPool();
            AtomicLong countCalls = new AtomicLong(0);
            AtomicInteger concurrentCalls = new AtomicInteger(0);
            BindableService service = new FrontierImplBase() {
                @Override
                public void crawlSeed(CrawlSeedRequest request, StreamObserver<CrawlExecutionId> responseObserver) {
                    int delayFactor = concurrentCalls.incrementAndGet();
                    CrawlExecutionId reply = CrawlExecutionId.newBuilder().setId("ceid" + countCalls.incrementAndGet()).build();
                    for (int i = 0; i < (1000 ^ delayFactor); i++) {
                    }
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                    concurrentCalls.decrementAndGet();
                }

                @Override
                public void busyCrawlHostGroupCount(Empty request, StreamObserver<CountResponse> responseObserver) {
                    responseObserver.onNext(CountResponse.newBuilder().setCount(10).build());
                    responseObserver.onCompleted();
                }

                @Override
                public void queueCountTotal(Empty request, StreamObserver<CountResponse> responseObserver) {
                    responseObserver.onNext(CountResponse.newBuilder().setCount(10).build());
                    responseObserver.onCompleted();
                }

                @Override
                public void queueCountForCrawlExecution(CrawlExecutionId request, StreamObserver<CountResponse> responseObserver) {
                    super.queueCountForCrawlExecution(request, responseObserver);
                }

                @Override
                public void queueCountForCrawlHostGroup(CrawlHostGroup request, StreamObserver<CountResponse> responseObserver) {
                    super.queueCountForCrawlHostGroup(request, responseObserver);
                }

            };

            ServerInterceptor interceptor = ConcurrencyLimitServerInterceptor.newBuilder(
                    new GrpcServerLimiterBuilder()
                            .partitionByMethod()
                            .partition(FrontierGrpc.getCrawlSeedMethod().getFullMethodName(), 0.38)
                            .partition(FrontierGrpc.getGetNextPageMethod().getFullMethodName(), 0.02)
                            .partition(FrontierGrpc.getBusyCrawlHostGroupCountMethod().getFullMethodName(), 0.15)
                            .partition(FrontierGrpc.getQueueCountForCrawlExecutionMethod().getFullMethodName(), 0.15)
                            .partition(FrontierGrpc.getQueueCountForCrawlHostGroupMethod().getFullMethodName(), 0.15)
                            .partition(FrontierGrpc.getQueueCountTotalMethod().getFullMethodName(), 0.15)
                            .limit(WindowedLimit.newBuilder()
                                    .build(Gradient2Limit.newBuilder()
                                            .build()))
                            .build())
                    .build();

            server = frontierServerBuilder.executor(executor)
                    .addService(ServerInterceptors.intercept(service, interceptor))
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

    private ConfigObject.Builder newConfObj(Kind kind, String name) {
        ConfigObject.Builder co = ConfigObject.newBuilder().setKind(kind).setId(name);
        co.getMetaBuilder().setName(name);
        return co;
    }
}
