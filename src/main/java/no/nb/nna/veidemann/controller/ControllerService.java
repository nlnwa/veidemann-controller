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

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.controller.v1.ControllerGrpc;
import no.nb.nna.veidemann.api.controller.v1.ExecutionId;
import no.nb.nna.veidemann.api.controller.v1.OpenIdConnectIssuerReply;
import no.nb.nna.veidemann.api.controller.v1.RoleList;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlReply;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.crawlSeed;
import static no.nb.nna.veidemann.controller.JobExecutionUtil.submitSeeds;

/**
 *
 */
public class ControllerService extends ControllerGrpc.ControllerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(ControllerService.class);

    private final ConfigAdapter db;
    private final ExecutionsAdapter executionsAdapter;

    private final Settings settings;

    public ControllerService(Settings settings) {
        this.settings = settings;
        this.db = DbService.getInstance().getConfigAdapter();
        this.executionsAdapter = DbService.getInstance().getExecutionsAdapter();
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.ADMIN})
    public void runCrawl(RunCrawlRequest request, StreamObserver<RunCrawlReply> responseObserver) {
        try {
            ConfigRef jobRequest = ConfigRef.newBuilder()
                    .setKind(Kind.crawlJob)
                    .setId(request.getJobId())
                    .build();

            ConfigObject job = db.getConfigObject(jobRequest);
            LOG.info("Job '{}' starting", job.getMeta().getName());

            JobExecutionStatus jobExecutionStatus;
            boolean addToRunningJob = false;

            JobExecutionsListReply runningJobsRequest = DbService.getInstance().getExecutionsAdapter()
                    .listJobExecutionStatus(ListJobExecutionsRequest.newBuilder()
                            .addState("RUNNING").build());

            List<JobExecutionStatus> runningJobs = runningJobsRequest.getValueList()
                    .stream()
                    .filter(jes -> jes.getJobId().equals(request.getJobId()))
                    .collect(Collectors.toList());

            if (runningJobs.isEmpty()) {
                jobExecutionStatus = DbService.getInstance().getExecutionsAdapter()
                        .createJobExecutionStatus(job.getId());
                LOG.info("Creating new job execution '{}'", jobExecutionStatus.getId());
            } else {
                jobExecutionStatus = runningJobs.get(0);
                addToRunningJob = true;
                LOG.info("Adding seeds to running job execution'{}'", jobExecutionStatus.getId());
            }

            RunCrawlReply reply = RunCrawlReply.newBuilder().setJobExecutionId(jobExecutionStatus.getId()).build();

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            if (!request.getSeedId().isEmpty()) {
                ConfigObject seed = db.getConfigObject(ConfigRef.newBuilder()
                        .setKind(Kind.seed)
                        .setId(request.getSeedId())
                        .build());
                crawlSeed(job, seed, jobExecutionStatus, addToRunningJob);
            } else {
                submitSeeds(job, jobExecutionStatus, addToRunningJob);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN})
    public void abortCrawlExecution(ExecutionId request, StreamObserver<CrawlExecutionStatus> responseObserver) {
        try {
            CrawlExecutionStatus status = executionsAdapter.setCrawlExecutionStateAborted(request.getId());

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN})
    public void abortJobExecution(ExecutionId request, StreamObserver<JobExecutionStatus> responseObserver) {
        try {
            JobExecutionStatus status = executionsAdapter.setJobExecutionStateAborted(request.getId());

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public void getRolesForActiveUser(Empty request, StreamObserver<RoleList> responseObserver) {
        try {
            Collection<Role> roles = RolesContextKey.roles()
                    .stream()
                    .map(r -> Role.valueOf(r.name()))
                    .collect(Collectors.toSet());
            if (roles == null) {
                responseObserver.onNext(RoleList.newBuilder().build());
            } else {
                responseObserver.onNext(RoleList.newBuilder().addAllRole(roles).build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public void getOpenIdConnectIssuer(Empty request, StreamObserver<OpenIdConnectIssuerReply> responseObserver) {
        try {
            LOG.debug("OpenIdConnectIssuer requested. Returning '{}'", settings.getOpenIdConnectIssuer());
            responseObserver.onNext(OpenIdConnectIssuerReply.newBuilder()
                    .setOpenIdConnectIssuer(settings.getOpenIdConnectIssuer()).build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

}
