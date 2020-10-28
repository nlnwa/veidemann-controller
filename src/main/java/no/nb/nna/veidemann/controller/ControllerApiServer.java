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

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.opentracing.contrib.ServerTracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.commons.auth.ApiKeyAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapper;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromConfig;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromFile;
import no.nb.nna.veidemann.commons.auth.AuthorisationAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.IdTokenAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.IdTokenValidator;
import no.nb.nna.veidemann.commons.auth.NoopAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.UserRoleMapper;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ControllerApiServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerApiServer.class);

    private final ServerBuilder<?> serverBuilder;
    private Server server;
    private final ExecutorService threadPool;
    final UserRoleMapper userRoleMapper;
    final Settings settings;

    public ControllerApiServer(Settings settings, UserRoleMapper userRoleMapper) {
        this(settings, ServerBuilder.forPort(settings.getApiPort()), userRoleMapper);
    }

    public ControllerApiServer(Settings settings, ServerBuilder<?> serverBuilder, UserRoleMapper userRoleMapper) {
        this.settings = settings;
        this.serverBuilder = serverBuilder;
        this.userRoleMapper = userRoleMapper;
        threadPool = Executors.newCachedThreadPool();
        serverBuilder.executor(threadPool);
    }

    public ControllerApiServer start() {
        ServerTracingInterceptor tracingInterceptor = new ServerTracingInterceptor.Builder(GlobalTracer.get())
                .withTracedAttributes(ServerTracingInterceptor.ServerRequestAttribute.CALL_ATTRIBUTES,
                        ServerTracingInterceptor.ServerRequestAttribute.METHOD_TYPE)
                .build();

        List<ServerInterceptor> interceptors = new ArrayList<>();
        interceptors.add(tracingInterceptor);
        try {
            interceptors = getAuAuServerInterceptors(interceptors);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Use secure transport if certChain and private key are available
        File certDir = new File("/veidemann/tls");
        File certChain = new File(certDir, "tls.crt");
        File privateKey = new File(certDir, "tls.key");
        if (certChain.isFile() && certChain.canRead() && privateKey.isFile() && privateKey.canRead()) {
            LOG.info("Found certificate. Setting up secure protocol.");
            serverBuilder.useTransportSecurity(certChain, privateKey);
        } else {
            LOG.warn("No CA certificate found. Protocol will use insecure plain text.");
        }

        server = serverBuilder
                .addService(createService(new ConfigService(), interceptors))
                .addService(createService(new ControllerService(settings), interceptors))
                .addService(createService(new ReportService(), interceptors))
                .addService(createService(new EventService(), interceptors))
                .build();

        try {
            server.start();

            LOG.info("Controller api listening on {}", server.getPort());

            return this;
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }
    }

    private ServerServiceDefinition createService(BindableService service, List<ServerInterceptor> interceptors) {
        return ServerInterceptors.interceptForward(new AuthorisationAuAuServerInterceptor(service).intercept(service), interceptors);
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                server.shutdown();
            }
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
            }
        }
        System.err.println("*** server shut down");
    }

    List<ServerInterceptor> getAuAuServerInterceptors(List<ServerInterceptor> interceptors) throws InterruptedException {
        if (settings.getSkipAuthentication()) {
            interceptors.add(new NoopAuAuServerInterceptor());
            return interceptors;
        }

        String issuerUrl = settings.getOpenIdConnectIssuer();
        if (issuerUrl != null && !issuerUrl.isEmpty()) {
            IdTokenValidator idTokenValidator = null;

            // Retry for 400 seconds if IDP doesn't respond
            int retryAttempts = 0;
            while (idTokenValidator == null && retryAttempts < 20) {
                try {
                    idTokenValidator = new IdTokenValidator(issuerUrl);
                } catch (Exception e) {
                    retryAttempts++;
                    Thread.sleep(20000);
                }
            }

            interceptors.add(new IdTokenAuAuServerInterceptor(userRoleMapper, idTokenValidator));
        }
        ApiKeyRoleMapper apiKeyDbRoleMapper = new ApiKeyRoleMapperFromConfig(userRoleMapper);
        interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyDbRoleMapper));

        ApiKeyRoleMapper apiKeyFileRoleMapper = new ApiKeyRoleMapperFromFile(settings.getApiKeyRoleMappingFile());
        interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyFileRoleMapper));
        return interceptors;
    }
}
