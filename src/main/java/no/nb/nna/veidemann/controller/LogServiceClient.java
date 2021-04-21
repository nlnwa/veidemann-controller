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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.log.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LogServiceClient implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogServiceClient.class);

    private final ManagedChannel channel;

    private final LogGrpc.LogBlockingStub blockingStub;
    private final LogGrpc.LogStub asyncStub;

    public LogServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("LogService client pointing to " + host + ":" + port);
    }

    public LogServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up log service client");
        channel = channelBuilder.build();
        blockingStub = LogGrpc.newBlockingStub(channel);
        asyncStub = LogGrpc.newStub(channel);
    }

    public Iterator<CrawlLog> listCrawlLog(CrawlLogListRequest request) {
        return blockingStub.listCrawlLogs(request);
    }

    public void listCrawlLog(CrawlLogListRequest request, StreamObserver<CrawlLog> responseObserver) {
        asyncStub.listCrawlLogs(request, responseObserver);
    }

    public Iterator<PageLog> listPageLogs(PageLogListRequest request) {
        return blockingStub.listPageLogs(request);
    }

    public void listPageLogs(PageLogListRequest request, StreamObserver<PageLog> responseObserver) {
        asyncStub.listPageLogs(request, responseObserver);
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
