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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentracing.contrib.ClientTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.CanonicalizeRequest;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.CanonicalizeResponse;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.UriCanonicalizerServiceGrpc;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.UriCanonicalizerServiceGrpc.UriCanonicalizerServiceBlockingStub;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.UriCanonicalizerServiceGrpc.UriCanonicalizerServiceFutureStub;
import no.nb.nna.veidemann.api.uricanonicalizer.v1.UriCanonicalizerServiceGrpc.UriCanonicalizerServiceStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScopeServiceClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ScopeServiceClient.class);

    private final ManagedChannel channel;

    private final UriCanonicalizerServiceBlockingStub blockingStub;

    private final UriCanonicalizerServiceStub asyncStub;

    private final UriCanonicalizerServiceFutureStub futureStub;

    public ScopeServiceClient(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        LOG.info("ScopesCheckerService client pointing to " + host + ":" + port);
    }

    public ScopeServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        LOG.info("Setting up scope service client");
        ClientTracingInterceptor tracingInterceptor = new ClientTracingInterceptor.Builder(GlobalTracer.get()).build();
        channel = channelBuilder.intercept(tracingInterceptor).build();
        blockingStub = UriCanonicalizerServiceGrpc.newBlockingStub(channel);
        asyncStub = UriCanonicalizerServiceGrpc.newStub(channel);
        futureStub = UriCanonicalizerServiceGrpc.newFutureStub(channel);
    }

    public ListenableFuture<CanonicalizeResponse> canonicalize(CanonicalizeRequest uri) {
        return futureStub.canonicalize(uri);
    }

    public String canonicalize(String uri) {
        return blockingStub.canonicalize(CanonicalizeRequest.newBuilder().setUri(uri).build()).getUri().getHref();
    }

    public void canonicalize(CanonicalizeRequest uri, FutureCallback<CanonicalizeResponse> callback, Executor executor) {
        ListenableFuture<CanonicalizeResponse> future = futureStub.canonicalize(uri);
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
