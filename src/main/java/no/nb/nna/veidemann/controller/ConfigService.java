/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigGrpc;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.DeleteResponse;
import no.nb.nna.veidemann.api.config.v1.GetLabelKeysRequest;
import no.nb.nna.veidemann.api.config.v1.GetScriptAnnotationsRequest;
import no.nb.nna.veidemann.api.config.v1.GetScriptAnnotationsResponse;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.LabelKeysResponse;
import no.nb.nna.veidemann.api.config.v1.ListCountResponse;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.LogLevels;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.config.v1.UpdateRequest;
import no.nb.nna.veidemann.api.config.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.auth.Authorisations;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;

public class ConfigService extends ConfigGrpc.ConfigImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigService.class);

    private final ConfigAdapter db;

    private final ScopeServiceClient scopeServiceClient;

    public ConfigService(ScopeServiceClient scopeServiceClient) {
        this.db = DbService.getInstance().getConfigAdapter();
        this.scopeServiceClient = scopeServiceClient;
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT})
    public void getConfigObject(ConfigRef request, StreamObserver<ConfigObject> responseObserver) {
        handleGet(() -> db.getConfigObject(request), responseObserver);
    }

    @Override
    @Authorisations({
            @AllowedRoles(value = {Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping}),
    })
    public void listConfigObjects(ListRequest request, StreamObserver<ConfigObject> observer) {
        StreamObserver<ConfigObject> responseObserver = new BlockingStreamObserver<>(observer);
        new Thread(() -> {
            try (ChangeFeed<ConfigObject> c = db.listConfigObjects(request);) {
                c.stream().forEach(o -> responseObserver.onNext(o));
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                LOG.error(e.getMessage(), e);
                responseObserver.onError(e);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
                Status status = Status.UNKNOWN.withDescription(ex.toString());
                responseObserver.onError(status.asException());
            }
        }).start();
    }

    @Override
    @Authorisations({
            @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping}),
    })
    public void countConfigObjects(ListRequest request, StreamObserver<ListCountResponse> responseObserver) {
        handleGet(() -> db.countConfigObjects(request), responseObserver);
    }

    @Override
    @Authorisations({
            @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping}),
            @AllowedRoles(value = {Role.CONSULTANT, Role.CURATOR, Role.OPERATOR, Role.ADMIN},
                    kind = {Kind.seed, Kind.crawlEntity})
    })
    public void saveConfigObject(ConfigObject request, StreamObserver<ConfigObject> responseObserver) {
        try {
            // If kind is seed, canonicalize uri
            if (request.getKind() == Kind.seed) {
                String canonicalizedUri = scopeServiceClient.canonicalize(request.getMeta().getName());
                if (!canonicalizedUri.equals(request.getMeta().getName())) {
                    ConfigObject.Builder b = request.toBuilder();
                    b.getMetaBuilder().setName(canonicalizedUri);
                    request = b.build();
                }
            }

            responseObserver.onNext(db.saveConfigObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @Authorisations({
            @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping}),
            @AllowedRoles(value = {Role.ADMIN, Role.CURATOR, Role.OPERATOR, Role.CONSULTANT},
                    kind = {Kind.crawlEntity, Kind.seed})
    })
    public void updateConfigObjects(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        try {
            responseObserver.onNext(db.updateConfigObjects(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @Authorisations({
            @AllowedRoles({Role.ADMIN}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping})
    })
    public void deleteConfigObject(ConfigObject request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            responseObserver.onNext(db.deleteConfigObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @Authorisations({
            @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT}),
            @AllowedRoles(value = {Role.ADMIN}, kind = {Kind.roleMapping})
    })
    public void getLabelKeys(GetLabelKeysRequest request, StreamObserver<LabelKeysResponse> responseObserver) {
        handleGet(() -> db.getLabelKeys(request), responseObserver);
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    @Authorisations({
            @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN}),
    })
    public void getScriptAnnotations(GetScriptAnnotationsRequest request, StreamObserver<GetScriptAnnotationsResponse> responseObserver) {
        handleGet(() -> {

            ConfigObject jobConfig = db.getConfigObject(request.getJob());
            Map<String, Annotation> annotations = JobExecutionUtil.GetScriptAnnotationsForJob(jobConfig);

            if (request.hasSeed()) {
                ConfigObject seed = db.getConfigObject(request.getSeed());
                annotations = JobExecutionUtil.GetScriptAnnotationOverridesForSeed(seed, jobConfig, annotations);
            }

            GetScriptAnnotationsResponse response = GetScriptAnnotationsResponse.newBuilder()
                    .addAllAnnotation(annotations.values())
                    .build();

            return response;
        }, responseObserver);
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN})
    public void saveLogConfig(LogLevels request, StreamObserver<LogLevels> responseObserver) {
        try {
            responseObserver.onNext(db.saveLogConfig(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.OPERATOR, Role.ADMIN})
    public void getLogConfig(Empty request, StreamObserver<LogLevels> responseObserver) {
        try {
            responseObserver.onNext(db.getLogConfig());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }
}
