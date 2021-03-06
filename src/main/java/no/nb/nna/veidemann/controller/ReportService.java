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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.net.Cursor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.report.v1.*;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.controller.query.QueryEngine;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ReportService extends ReportGrpc.ReportImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(ReportService.class);

    private final ExecutionsAdapter executionsAdapter;

    private final Gson gson;

    public ReportService() {
        this.executionsAdapter = DbService.getInstance().getExecutionsAdapter();
        gson = new GsonBuilder()
                .create();
    }

    @Override
    public void listExecutions(CrawlExecutionsListRequest request, StreamObserver<CrawlExecutionStatus> observer) {
        StreamObserver<CrawlExecutionStatus> responseObserver = new BlockingStreamObserver<>(observer);
        new Thread(() -> {
            try (ChangeFeed<CrawlExecutionStatus> c = executionsAdapter.listCrawlExecutionStatus(request);) {
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
    public void listJobExecutions(JobExecutionsListRequest request, StreamObserver<JobExecutionStatus> observer) {
        StreamObserver<JobExecutionStatus> responseObserver = new BlockingStreamObserver<>(observer);
        new Thread(() -> {
            try (ChangeFeed<JobExecutionStatus> c = executionsAdapter.listJobExecutionStatus(request);) {
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
    @AllowedRoles({Role.OPERATOR, Role.ADMIN, Role.CURATOR, Role.CONSULTANT})
    public void executeDbQuery(ExecuteDbQueryRequest request, StreamObserver<ExecuteDbQueryReply> respObserver) {
        try {
            ReqlAst qry = QueryEngine.getInstance().parseQuery(request.getQuery());
            int limit = request.getLimit();

            // Default limit
            if (limit == 0) {
                limit = 50;
            }

            RethinkDbConnection conn = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
            Object result = conn.exec("js-query", qry);
            if (result != null) {
                if (result instanceof Cursor) {
                    try (Cursor c = (Cursor) result) {
                        int index = 0;
                        while (!((ServerCallStreamObserver) respObserver).isCancelled()
                                && c.hasNext() && (limit == -1 || index++ < limit)) {
                            try {
                                Object r = c.next(1000);
                                respObserver.onNext(recordToExecuteDbQueryReply(r));
                            } catch (TimeoutException e) {
                                // Timeout is ok
                            }
                        }
                    }
                } else {
                    respObserver.onNext(recordToExecuteDbQueryReply(result));
                }
            }

            respObserver.onCompleted();
        } catch (Exception e) {
            LOG.debug(e.getMessage(), e);
            Status status = Status.UNKNOWN.withDescription(e.toString());
            respObserver.onError(status.asException());
        }
    }

    private ExecuteDbQueryReply recordToExecuteDbQueryReply(Object record) {
        return ExecuteDbQueryReply.newBuilder()
                .setRecord(gson.toJson(record))
                .build();
    }
}
