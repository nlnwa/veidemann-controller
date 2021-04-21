package no.nb.nna.veidemann.controller;

import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.log.v1.*;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;

public class LogService extends LogGrpc.LogImplBase {

    private final LogServiceClient logServiceClient;

    public LogService(LogServiceClient logServiceClient) {
        this.logServiceClient = logServiceClient;
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT})
    public void listCrawlLogs(CrawlLogListRequest request, StreamObserver<CrawlLog> responseObserver) {
        this.logServiceClient.listCrawlLog(request, responseObserver);
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.CONSULTANT})
    public void listPageLogs(PageLogListRequest request, StreamObserver<PageLog> responseObserver) {
        this.logServiceClient.listPageLogs(request, responseObserver);
    }
}
