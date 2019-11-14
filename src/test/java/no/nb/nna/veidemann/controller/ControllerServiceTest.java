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
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.commons.auth.ApiKeyAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapper;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromConfig;
import no.nb.nna.veidemann.commons.auth.ApiKeyRoleMapperFromFile;
import no.nb.nna.veidemann.commons.auth.AuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.IdTokenAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.IdTokenValidator;
import no.nb.nna.veidemann.commons.auth.UserRoleMapper;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyCollection;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ControllerServiceTest {

    private final String uniqueServerName = "in-process server for " + getClass();

    private InProcessServerBuilder inProcessServerBuilder;

    private ControllerApiServerMock inProcessServer;

    private ManagedChannel inProcessChannel;

    private ConfigGrpc.ConfigBlockingStub blockingStub;

    private ConfigGrpc.ConfigStub asyncStub;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void beforeEachTest() {
        inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName).directExecutor();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
        blockingStub = ConfigGrpc.newBlockingStub(inProcessChannel);
        asyncStub = ConfigGrpc.newStub(inProcessChannel);
    }

    @After
    public void afterEachTest() {
        inProcessChannel.shutdownNow();
        inProcessServer.close();
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

        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        ConfigAdapter dbMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(dbMock);
        try (DbService db = DbService.configure(dbProviderMock)) {
            IdTokenValidator idValidatorMock = mock(IdTokenValidator.class);
            UserRoleMapper roleMapperMock = mock(UserRoleMapper.class);

            ConfigObject coEntity = ConfigObject.newBuilder()
                    .setKind(Kind.crawlEntity)
                    .build();

            ConfigRef entityRef = ConfigRef.newBuilder()
                    .setKind(Kind.crawlEntity)
                    .build();

            when(dbMock.getConfigObject(entityRef))
                    .thenReturn(coEntity);
            when(dbMock.saveConfigObject(coEntity))
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

            inProcessServer = new ControllerApiServerMock(idValidatorMock, inProcessServerBuilder, roleMapperMock).start();

            assertThat(blockingStub.withCallCredentials(cred).getConfigObject(entityRef))
                    .isSameAs(coEntity);

            thrown.expect(StatusRuntimeException.class);
            thrown.expectMessage("PERMISSION_DENIED");
            blockingStub.withCallCredentials(cred).saveConfigObject(coEntity);

            thrown.expectMessage("UNAUTHENTICATED");
            blockingStub.saveConfigObject(coEntity);
        }
    }

    private class ControllerApiServerMock extends ControllerApiServer {
        final IdTokenValidator idValidator;

        @Override
        public ControllerApiServerMock start() {
            super.start();
            return this;
        }

        public ControllerApiServerMock(IdTokenValidator idValidator, ServerBuilder<?> serverBuilder, UserRoleMapper userRoleMapper) {
            super(new Settings(), serverBuilder, userRoleMapper);
            this.idValidator = idValidator;
        }

        @Override
        List<ServerInterceptor> getAuAuServerInterceptors(List<ServerInterceptor> interceptors) throws InterruptedException {
            interceptors.add(new IdTokenAuAuServerInterceptor(userRoleMapper, idValidator));

            ApiKeyRoleMapper apiKeyDbRoleMapper = new ApiKeyRoleMapperFromConfig(userRoleMapper);
            interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyDbRoleMapper));

            ApiKeyRoleMapper apiKeyFileRoleMapper = new ApiKeyRoleMapperFromFile(settings.getApiKeyRoleMappingFile());
            interceptors.add(new ApiKeyAuAuServerInterceptor(apiKeyFileRoleMapper));
            return interceptors;
        }
    }
}
