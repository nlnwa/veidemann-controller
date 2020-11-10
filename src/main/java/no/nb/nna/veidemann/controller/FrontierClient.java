/*
 * Copyright 2017 National Library of Norway.
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc.FrontierBlockingStub;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc.FrontierFutureStub;
import no.nb.nna.veidemann.api.frontier.v1.FrontierGrpc.FrontierStub;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.client.GrpcUtil;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FrontierClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FrontierClient.class);

    private final ManagedChannel channel;

    private final FrontierBlockingStub blockingStub;

    private final FrontierStub asyncStub;

    private final FrontierFutureStub futureStub;

    public FrontierClient(final String host, final int port, String supportedSeedType) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(), supportedSeedType);
        LOG.info("Frontier client pointing to " + host + ":" + port);
    }

    public FrontierClient(ManagedChannelBuilder<?> channelBuilder, String supportedSeedType) {
        LOG.info("Setting up Frontier client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = FrontierGrpc.newBlockingStub(channel);
        asyncStub = FrontierGrpc.newStub(channel);
        futureStub = FrontierGrpc.newFutureStub(channel);
        JobExecutionUtil.addFrontierClient(supportedSeedType, this);
    }

    /**
     * Request Frontier to craw a Seed.
     *
     * @param crawlJob     the crawl job configuration
     * @param seed         the seed to crawl
     * @param jobExecution the jobExecution this crawl is part of
     * @param annotations
     * @param timeout      timestamp for when this crawl times out. Might be null for no timeout
     * @return the id of the newly created crawl execution
     */
    public CrawlExecutionId crawlSeed(ConfigObject crawlJob, ConfigObject seed, JobExecutionStatus jobExecution,
                                      Collection<Annotation> annotations, OffsetDateTime timeout) {
        try {
            CrawlSeedRequest.Builder request = CrawlSeedRequest.newBuilder()
                    .setJob(crawlJob)
                    .setSeed(seed)
                    .setJobExecutionId(jobExecution.getId())
                    .addAllAnnotation(annotations);
            if (timeout != null) {
                request.setTimeout(ProtoUtils.odtToTs(timeout));
            }
            return GrpcUtil.forkedCall(() -> blockingStub.crawlSeed(request.build()));
        } catch (StatusRuntimeException ex) {
            LOG.error("RPC failed: " + ex.getStatus(), ex);
            throw ex;
        }
    }

    public ListenableFuture<CountResponse> busyCrawlHostGroupCount() {
        return futureStub.busyCrawlHostGroupCount(Empty.getDefaultInstance());
    }

    public ListenableFuture<CountResponse> queueCountTotal() {
        return futureStub.queueCountTotal(Empty.getDefaultInstance());
    }

    public void queueCountForCrawlExecution(CrawlExecutionId crawlExecutionId, FutureCallback<CountResponse> callback, Executor executor) {
        ListenableFuture<CountResponse> future = futureStub.queueCountForCrawlExecution(crawlExecutionId);
        Futures.addCallback(future, callback, executor);
    }

    public void queueCountForCrawlHostGroup(CrawlHostGroup crawlHostGroup, FutureCallback<CountResponse> callback, Executor executor) {
        ListenableFuture<CountResponse> future = futureStub.queueCountForCrawlHostGroup(crawlHostGroup);
        Futures.addCallback(future, callback, executor);
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            channel.shutdownNow();
        }
    }

}
