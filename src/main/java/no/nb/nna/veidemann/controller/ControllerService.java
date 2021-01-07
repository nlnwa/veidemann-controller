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
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.controller.v1.ControllerGrpc;
import no.nb.nna.veidemann.api.controller.v1.CrawlerStatus;
import no.nb.nna.veidemann.api.controller.v1.CrawlerStatus.Builder;
import no.nb.nna.veidemann.api.controller.v1.ExecutionId;
import no.nb.nna.veidemann.api.controller.v1.OpenIdConnectIssuerReply;
import no.nb.nna.veidemann.api.controller.v1.RoleList;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlReply;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.controller.v1.RunStatus;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.auth.RolesContextKey;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.calculateTimeout;
import static no.nb.nna.veidemann.controller.JobExecutionUtil.crawlSeed;

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
    @AllowedRoles({Role.CURATOR, Role.ADMIN, Role.OPERATOR})
    public void runCrawl(RunCrawlRequest request, StreamObserver<RunCrawlReply> responseObserver) {
        try {
            ConfigRef jobRequest = ConfigRef.newBuilder()
                    .setKind(Kind.crawlJob)
                    .setId(request.getJobId())
                    .build();

            ConfigObject job = db.getConfigObject(jobRequest);
            LOG.info("Job '{}' starting", job.getMeta().getName());

            JobExecutionStatus jobExecutionStatus = null;
            boolean addToRunningJob = false;

            ChangeFeed<JobExecutionStatus> runningJobsR = DbService.getInstance().getExecutionsAdapter()
                    .listJobExecutionStatus(JobExecutionsListRequest.newBuilder()
                            .addState(State.RUNNING).build());

            List<JobExecutionStatus> runningJobs = runningJobsR
                    .stream()
                    .filter(jes -> jes.getJobId().equals(request.getJobId()))
                    .collect(Collectors.toList());

            if (!runningJobs.isEmpty()) {
                jobExecutionStatus = runningJobs.get(0);
                addToRunningJob = true;
                LOG.info("Adding seeds to running job execution'{}'", jobExecutionStatus.getId());
            }

            OffsetDateTime timeout = calculateTimeout(job);

            if (!request.getSeedId().isEmpty()) {
                jobExecutionStatus = JobExecutionUtil.createJobExecutionStatusIfNotExist(job, jobExecutionStatus);
                ConfigObject seed = db.getConfigObject(ConfigRef.newBuilder()
                        .setKind(Kind.seed)
                        .setId(request.getSeedId())
                        .build());
                Map<String, Annotation> jobAnnotations = JobExecutionUtil.GetScriptAnnotationsForJob(job);
                crawlSeed(null, job, seed, jobExecutionStatus, jobAnnotations, timeout, addToRunningJob);
            } else {
                jobExecutionStatus = JobExecutionUtil.submitSeeds(job, jobExecutionStatus, timeout, addToRunningJob);
            }

            RunCrawlReply.Builder reply = RunCrawlReply.newBuilder();
            if (jobExecutionStatus != null) {
                reply.setJobExecutionId(jobExecutionStatus.getId());
            }
            responseObserver.onNext(reply.build());
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN, Role.CURATOR})
    public void abortCrawlExecution(ExecutionId request, StreamObserver<CrawlExecutionStatus> responseObserver) {
        try {
            CrawlExecutionStatus status = executionsAdapter.setCrawlExecutionStateAborted(
                    request.getId(), CrawlExecutionStatus.State.ABORTED_MANUAL);

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN, Role.CURATOR})
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
            Collection<Role> roles = RolesContextKey.roles();
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

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN})
    public void pauseCrawler(Empty request, StreamObserver<Empty> responseObserver) {
        try {
            executionsAdapter.setDesiredPausedState(true);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN})
    public void unPauseCrawler(Empty request, StreamObserver<Empty> responseObserver) {
        try {
            executionsAdapter.setDesiredPausedState(false);
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.OPERATOR, Role.ADMIN, Role.CONSULTANT, Role.CURATOR, Role.ANY_USER})
    public void status(Empty request, StreamObserver<CrawlerStatus> responseObserver) {
        try {
            boolean desiredPausedState = executionsAdapter.getDesiredPausedState();
            JobExecutionUtil.queueCountAndBusyChgCount(new FutureCallback<Builder>() {
                @Override
                public void onSuccess(@Nullable Builder result) {
                    boolean isPaused = result.getBusyCrawlHostGroupCount() == 0;

                    RunStatus runStatus = desiredPausedState && isPaused
                            ? RunStatus.PAUSED
                            : desiredPausedState ? RunStatus.PAUSE_REQUESTED : RunStatus.RUNNING;

                    responseObserver.onNext(result
                            .setRunStatus(runStatus)
                            .build());
                    responseObserver.onCompleted();
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.error(t.getMessage(), t);
                    Status status = Status.UNKNOWN.withDescription(t.toString());
                    responseObserver.onError(status.asException());
                }
            });
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    public void queueCountForCrawlExecution(CrawlExecutionId request, StreamObserver<CountResponse> responseObserver) {
        JobExecutionUtil.queueCountForCrawlExecution(request, new FutureCallback<CountResponse>() {
            @Override
            public void onSuccess(@Nullable CountResponse result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error(t.getMessage(), t);
                Status status = Status.UNKNOWN.withDescription(t.toString());
                responseObserver.onError(status.asException());
            }
        });
    }

    @Override
    public void queueCountForCrawlHostGroup(CrawlHostGroup request, StreamObserver<CountResponse> responseObserver) {
        JobExecutionUtil.queueCountForCrawlHostGroup(request, new FutureCallback<CountResponse>() {
            @Override
            public void onSuccess(@Nullable CountResponse result) {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                LOG.error(t.getMessage(), t);
                Status status = Status.UNKNOWN.withDescription(t.toString());
                responseObserver.onError(status.asException());
            }
        });
    }
}
