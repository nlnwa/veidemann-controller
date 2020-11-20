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

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import com.nimbusds.jwt.JWTClaimsSet;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import net.minidev.json.JSONArray;
import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.BrowserConfig;
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.config.v1.Seed;
import no.nb.nna.veidemann.api.controller.v1.ControllerGrpc;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlReply;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.auth.ApiKeyAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapper;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromConfig;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromFile;
import no.nb.nna.veidemann.commons.auth.AuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.IdTokenAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.IdTokenValidator;
import no.nb.nna.veidemann.commons.auth.NoopAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.UserRoleMapper;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.controller.settings.Settings;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ControllerServiceTest {
    private final String uniqueServerName = "in-process server for " + getClass();

    private InProcessServerBuilder inProcessServerBuilder;
    private ControllerApiServerMock inProcessServer;
    private ManagedChannel inProcessChannel;

    private ConfigGrpc.ConfigBlockingStub configClient;
    private ControllerGrpc.ControllerBlockingStub controllerClient;

    private ConfigAdapter configAdapterMock;
    private ExecutionsAdapter executionsAdapterMock;
    private List<CrawlSeedRequest> frontierInvocations;

    private DbService db;

    @BeforeEach
    public void beforeEachTest() throws DbException {
        inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName).directExecutor();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
        configClient = ConfigGrpc.newBlockingStub(inProcessChannel);
        controllerClient = ControllerGrpc.newBlockingStub(inProcessChannel);

        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        configAdapterMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(configAdapterMock);
        executionsAdapterMock = mock(ExecutionsAdapter.class);
        when(dbProviderMock.getExecutionsAdapter()).thenReturn(executionsAdapterMock);

        db = DbService.configure(dbProviderMock);
        FrontierClient frontierClientMock = mock(FrontierClient.class);
        JobExecutionUtil.addFrontierClient("url", frontierClientMock);
        frontierInvocations = new ArrayList<>();
        when(frontierClientMock.crawlSeed(any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    try {
                        CrawlSeedRequest.Builder request = CrawlSeedRequest.newBuilder()
                                .setJob((ConfigObject) invocation.getArgument(0))
                                .setSeed((ConfigObject) invocation.getArgument(1))
                                .setJobExecutionId(((JobExecutionStatus) invocation.getArgument(2)).getId())
                                .addAllAnnotation(invocation.getArgument(3));
                        if (invocation.getArgument(4) != null) {
                            request.setTimeout(ProtoUtils.odtToTs(invocation.getArgument(4)));
                        }

                        frontierInvocations.add(request.build());
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                    return CrawlExecutionId.newBuilder().setId("ceid1").build();
                });

        ConfigObject entity = newConfObj(Kind.browserScript, "en1", ann("bs3", "bs3valFromEntity")).build();
        ConfigRef entityRef = ApiTools.refForConfig(entity);
        when(configAdapterMock.getConfigObject(entityRef))
                .thenReturn(entity);

        ConfigObject script1 = newConfObj(Kind.browserScript, "bs1", ann("bs1", "bs1valFromScript")).build();
        ConfigRef script1Ref = ApiTools.refForConfig(script1);
        when(configAdapterMock.getConfigObject(script1Ref))
                .thenReturn(script1);

        ConfigObject script2 = newConfObj(Kind.browserScript, "bs2", ann("bs2", "bs2valFromScript")).build();
        ConfigRef script2Ref = ApiTools.refForConfig(script2);
        when(configAdapterMock.getConfigObject(script2Ref))
                .thenReturn(script2);

        ConfigObject script3 = newConfObj(Kind.browserScript, "bs3", ann("bs3", "bs3valFromScript")).build();
        ConfigRef script3Ref = ApiTools.refForConfig(script3);
        when(configAdapterMock.getConfigObject(script3Ref))
                .thenReturn(script3);

        ConfigObject scopescript = newConfObj(Kind.browserScript, "scp", ann("scp", "scpvalFromScript")).build();
        ConfigRef scopescriptRef = ApiTools.refForConfig(scopescript);
        when(configAdapterMock.getConfigObject(scopescriptRef))
                .thenReturn(scopescript);

        ConfigObject browserConfig = newConfObj(Kind.browserConfig, "bc")
                .setBrowserConfig(BrowserConfig.newBuilder().addScriptRef(script1Ref).addScriptSelector("scope:default"))
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

        ConfigObject.Builder jobConfigB = newConfObj(Kind.crawlJob, "job1", ann("scp", "scpvalFromJob"));
        jobConfigB.getCrawlJobBuilder()
                .setCrawlConfigRef(crawlConfigRef)
                .setScopeScriptRef(scopescriptRef);
        ConfigObject jobConfig1 = jobConfigB.build();
        ConfigRef jobConfig1Ref = ApiTools.refForConfig(jobConfig1);
        when(configAdapterMock.getConfigObject(jobConfig1Ref))
                .thenReturn(jobConfig1);

        jobConfigB = newConfObj(Kind.crawlJob, "job2");
        jobConfigB.getCrawlJobBuilder()
                .setCrawlConfigRef(crawlConfigRef)
                .setScopeScriptRef(scopescriptRef);
        ConfigObject jobConfig2 = jobConfigB.build();
        ConfigRef jobConfig2Ref = ApiTools.refForConfig(jobConfig2);
        when(configAdapterMock.getConfigObject(jobConfig2Ref))
                .thenReturn(jobConfig2);

        ConfigObject.Builder seedB = newConfObj(Kind.seed, "seed1", ann("bs1", "bs1valFromSeed"), ann("{job2}bs2", "bs2valFromSeed"));
        seedB.getSeedBuilder()
                .setEntityRef(entityRef)
                .addJobRef(jobConfig1Ref);
        ConfigObject seed1 = seedB.build();
        ConfigRef seed1Ref = ApiTools.refForConfig(seed1);
        when(configAdapterMock.getConfigObject(seed1Ref))
                .thenReturn(seed1);

        seedB = newConfObj(Kind.seed, "seed2", ann("bs1", "bs1valFromSeed"), ann("{job1}bs2", "bs2valFromSeed"));
        seedB.getSeedBuilder()
                .setEntityRef(entityRef)
                .addJobRef(jobConfig1Ref);
        ConfigObject seed2 = seedB.build();
        ConfigRef seed2Ref = ApiTools.refForConfig(seed2);
        when(configAdapterMock.getConfigObject(seed2Ref))
                .thenReturn(seed2);

        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder().setKind(Kind.browserScript).addLabelSelector("scope:default").build()))
                .thenReturn(new ArrayChangeFeed<>(script2, script3));
        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setQueryTemplate(ConfigObject.newBuilder().setSeed(Seed.newBuilder().addJobRef(jobConfig1Ref)))
                .setQueryMask(FieldMask.newBuilder().addPaths("seed.jobRef"))
                .build()))
                .thenReturn(new ArrayChangeFeed<>(seed1, seed2));
        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setQueryTemplate(ConfigObject.newBuilder().setSeed(Seed.newBuilder().addJobRef(jobConfig2Ref)))
                .setQueryMask(FieldMask.newBuilder().addPaths("seed.jobRef"))
                .build()))
                .thenReturn(new ArrayChangeFeed<>());

        when(executionsAdapterMock.listJobExecutionStatus(any()))
                .thenReturn(new ArrayChangeFeed<>());

        when(executionsAdapterMock.createJobExecutionStatus("job1"))
                .thenReturn(JobExecutionStatus.newBuilder().setId("job1Execution1").build())
                .thenReturn(JobExecutionStatus.newBuilder().setId("job1Execution2").build());
        when(executionsAdapterMock.createJobExecutionStatus("job2"))
                .thenReturn(JobExecutionStatus.newBuilder().setId("job2Execution1").build());
    }

    @AfterEach
    public void afterEachTest() {
        if (db != null) {
            db.close();
        }
        inProcessChannel.shutdownNow();
        if (inProcessServer != null) {
            inProcessServer.close();
        }
    }

    @Test
    public void testAuthorization() throws DbException {
        CallCredentials cred = new CallCredentials() {
            @Override
            public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                Metadata headers = new Metadata();
                headers.put(AuAuServerInterceptor.AUTHORIZATION_KEY, "Bearer token1");
                applier.apply(headers);
            }

            @Override
            public void thisUsesUnstableApi() {

            }
        };

        IdTokenValidator idValidatorMock = mock(IdTokenValidator.class);
        UserRoleMapper roleMapperMock = mock(UserRoleMapper.class);

        ConfigObject coEntity = ConfigObject.newBuilder()
                .setKind(Kind.crawlEntity)
                .build();

        ConfigRef entityRef = ConfigRef.newBuilder()
                .setKind(Kind.crawlEntity)
                .build();

        when(configAdapterMock.getConfigObject(entityRef))
                .thenReturn(coEntity);
        when(configAdapterMock.saveConfigObject(coEntity))
                .thenAnswer((Answer<ConfigObject>) invocation -> {
                    assertThat(EmailContextKey.email()).isEqualTo("user@example.com");
                    return coEntity;
                });
        when(idValidatorMock.verifyIdToken("token1"))
                .thenReturn(new JWTClaimsSet.Builder()
                        .claim("email", "user@example.com")
                        .claim("groups", new JSONArray())
                        .build());
        when(roleMapperMock.getRolesForUser(eq("user@example.com"), anyList(), anyCollection()))
                .thenReturn(Lists.newArrayList(Role.ANY_USER, Role.READONLY));

        inProcessServer = new ControllerApiServerMock(new Settings(), idValidatorMock, inProcessServerBuilder, roleMapperMock).start();

        assertThat(configClient.withCallCredentials(cred).getConfigObject(entityRef))
                .isSameAs(coEntity);

        assertThatThrownBy(() -> configClient.withCallCredentials(cred).saveConfigObject(coEntity))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessage("PERMISSION_DENIED");

        assertThatThrownBy(() -> configClient.saveConfigObject(coEntity))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessage("UNAUTHENTICATED");
    }

    @Test
    public void testSkipAuthentication() throws DbException {
        ConfigObject coEntity = ConfigObject.newBuilder()
                .setKind(Kind.crawlEntity)
                .build();

        ConfigRef entityRef = ConfigRef.newBuilder()
                .setKind(Kind.crawlEntity)
                .build();

        when(configAdapterMock.getConfigObject(entityRef))
                .thenReturn(coEntity);
        when(configAdapterMock.saveConfigObject(coEntity))
                .thenAnswer((Answer<ConfigObject>) invocation -> {
                    assertThat(EmailContextKey.email()).isEqualTo(null);
                    return coEntity;
                });

        Settings settings = new Settings();
        settings.setSkipAuthentication(true);
        inProcessServer = new ControllerApiServerMock(settings, null, inProcessServerBuilder, null).start();

        assertThat(configClient.getConfigObject(entityRef))
                .isSameAs(coEntity);

        assertThat(configClient.saveConfigObject(coEntity))
                .isSameAs(coEntity);
    }

    @Test
    void runCrawlWithSeed() throws DbException, InterruptedException {
        Settings settings = new Settings();
        settings.setSkipAuthentication(true);
        inProcessServer = new ControllerApiServerMock(settings, null, inProcessServerBuilder, null).start();

        // Test crawl of specific seed
        RunCrawlRequest request = RunCrawlRequest.newBuilder().setJobId("job1").setSeedId("seed1").build();
        RunCrawlReply reply = controllerClient.runCrawl(request);
        assertThat(reply.getJobExecutionId()).isEqualTo("job1Execution1");

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(500);
        assertThat(frontierInvocations).hasSize(1);

        assertThat(frontierInvocations.get(0).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(0).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(0).getTimeout()).isInstanceOf(Timestamp.class);

        Annotation expected1 = Annotation.newBuilder().setKey("bs1").setValue("bs1valFromSeed").build();
        Annotation expected2 = Annotation.newBuilder().setKey("bs2").setValue("bs2valFromScript").build();
        Annotation expected3 = Annotation.newBuilder().setKey("bs3").setValue("bs3valFromEntity").build();
        Annotation expected4 = Annotation.newBuilder().setKey("scp").setValue("scpvalFromJob").build();
        assertThat(frontierInvocations.get(0).getAnnotationList())
                .containsExactlyInAnyOrder(expected1, expected2, expected3, expected4);

        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
    }

    @Test
    void runCrawl() throws DbException, InterruptedException {
        Settings settings = new Settings();
        settings.setSkipAuthentication(true);
        inProcessServer = new ControllerApiServerMock(settings, null, inProcessServerBuilder, null).start();

        // Test crawl all seeds for a job
        RunCrawlRequest request = RunCrawlRequest.newBuilder().setJobId("job1").build();
        RunCrawlReply reply = controllerClient.runCrawl(request);
        assertThat(reply.getJobExecutionId()).isEqualTo("job1Execution1");

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(500);
        assertThat(frontierInvocations).hasSize(2);

        assertThat(frontierInvocations.get(0).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(0).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(0).getTimeout()).isInstanceOf(Timestamp.class);
        assertThat(frontierInvocations.get(1).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(1).getSeed().getId()).isEqualTo("seed2");
        assertThat(frontierInvocations.get(1).getTimeout()).isInstanceOf(Timestamp.class);

        Annotation expected1 = Annotation.newBuilder().setKey("bs1").setValue("bs1valFromSeed").build();
        Annotation expected2 = Annotation.newBuilder().setKey("bs2").setValue("bs2valFromScript").build();
        Annotation expected3 = Annotation.newBuilder().setKey("bs3").setValue("bs3valFromEntity").build();
        Annotation expected4 = Annotation.newBuilder().setKey("scp").setValue("scpvalFromJob").build();
        Annotation expected5 = Annotation.newBuilder().setKey("bs2").setValue("bs2valFromSeed").build();

        assertThat(frontierInvocations.get(0).getAnnotationList())
                .containsExactlyInAnyOrder(expected1, expected2, expected3, expected4);
        assertThat(frontierInvocations.get(1).getAnnotationList())
                .containsExactlyInAnyOrder(expected1, expected3, expected4, expected5);

        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
    }

    @Test
    void runCrawlSeedsMissing() throws DbException, InterruptedException {
        Settings settings = new Settings();
        settings.setSkipAuthentication(true);
        inProcessServer = new ControllerApiServerMock(settings, null, inProcessServerBuilder, null).start();

        RunCrawlRequest request = RunCrawlRequest.newBuilder().setJobId("job2").build();
        RunCrawlReply reply = controllerClient.runCrawl(request);
        assertThat(reply.getJobExecutionId()).isEmpty();

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(500);
        assertThat(frontierInvocations).hasSize(0);

        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
    }

    private class ControllerApiServerMock extends ControllerApiServer {
        final IdTokenValidator idValidator;

        @Override
        public ControllerApiServerMock start() {
            super.start();
            return this;
        }

        public ControllerApiServerMock(Settings settings, IdTokenValidator idValidator, ServerBuilder<?> serverBuilder, UserRoleMapper userRoleMapper) {
            super(settings, serverBuilder, userRoleMapper, null);
            this.idValidator = idValidator;
        }

        @Override
        List<ServerInterceptor> getAuAuServerInterceptors(List<ServerInterceptor> interceptors) throws InterruptedException {
            if (settings.getSkipAuthentication()) {
                interceptors.add(new NoopAuAuServerInterceptor());
                return interceptors;
            }

            interceptors.add(new IdTokenAuAuServerInterceptor(userRoleMapper, idValidator));

            ApiKeyRoleMapper apiKeyDbRoleMapper = new ApiKeyRoleMapperFromConfig(userRoleMapper);
            interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyDbRoleMapper));

            ApiKeyRoleMapper apiKeyFileRoleMapper = new ApiKeyRoleMapperFromFile(settings.getApiKeyRoleMappingFile());
            interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyFileRoleMapper));
            return interceptors;
        }
    }

    ////////////////////
    // Helper methods //
    ////////////////////

    private ConfigObject.Builder newConfObj(Kind kind, String name, Annotation... annotations) {
        ConfigObject.Builder co = ConfigObject.newBuilder().setKind(kind).setId(name);
        co.getMetaBuilder().setName(name);
        for (Annotation a : annotations) {
            co.getMetaBuilder().addAnnotation(a);
        }
        return co;
    }

    private Annotation ann(String key, String value) {
        return Annotation.newBuilder().setKey(key).setValue(value).build();
    }
}
