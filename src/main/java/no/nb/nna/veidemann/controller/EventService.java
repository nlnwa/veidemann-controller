package no.nb.nna.veidemann.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.eventhandler.v1.DeleteResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.EventHandlerGrpc;
import no.nb.nna.veidemann.api.eventhandler.v1.EventObject;
import no.nb.nna.veidemann.api.eventhandler.v1.EventRef;
import no.nb.nna.veidemann.api.eventhandler.v1.ListCountResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.ListLabelRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.ListLabelResponse;
import no.nb.nna.veidemann.api.eventhandler.v1.ListRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.SaveRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateRequest;
import no.nb.nna.veidemann.api.eventhandler.v1.UpdateResponse;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.EventAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.handleGet;

public class EventService extends EventHandlerGrpc.EventHandlerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(EventService.class);

    private final EventAdapter db;

    public EventService() {
        this.db = DbService.getInstance().getEventAdapter();
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN})
    public void getEventObject(EventRef request, StreamObserver<EventObject> responseObserver) {
        handleGet(() -> db.getEventObject(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN})
    public void listEventObjects(ListRequest request, StreamObserver<EventObject> responseObserver) {
        new Thread(() -> {
            try (ChangeFeed<EventObject> c = db.listEventObjects(request);) {
                c.stream().forEach(o -> responseObserver.onNext(o));
                responseObserver.onCompleted();
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
                Status status = Status.UNKNOWN.withDescription(ex.toString());
                responseObserver.onError(status.asException());
            }
        }).start();
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.OPERATOR, Role.ADMIN})
    public void countEventObjects(ListRequest request, StreamObserver<ListCountResponse> responseObserver) {
        handleGet(() -> db.countEventObjects(request), responseObserver);
    }


    @Override
    @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN, Role.SYSTEM})
    public void saveEventObject(SaveRequest request, StreamObserver<EventObject> responseObserver) {
        try {
            responseObserver.onNext(db.saveEventObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.CURATOR, Role.OPERATOR, Role.ADMIN})
    public void updateEventObjects(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        try {
            responseObserver.onNext(db.updateEventObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ADMIN})
    public void deleteEventObject(EventObject request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            responseObserver.onNext(db.deleteEventObject(request));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    @Override
    @AllowedRoles({Role.ANY_USER})
    public void listLabels(ListLabelRequest request, StreamObserver<ListLabelResponse> responseObserver) {
        handleGet(() -> db.listLabels(request), responseObserver);
    }
}
