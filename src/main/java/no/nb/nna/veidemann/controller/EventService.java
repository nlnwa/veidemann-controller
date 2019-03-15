package no.nb.nna.veidemann.controller;

import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.Role;
import no.nb.nna.veidemann.api.eventhandler.v1.*;
import no.nb.nna.veidemann.commons.auth.AllowedRoles;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

public class EventService extends EventHandlerGrpc.EventHandlerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(EventService.class);

    //  private final EventAdapter db;

    // public EventService() { this.db = DbService.getInstance().getConfigAdapter(); }

    public EventService() {
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void getEventObject(EventRef request, StreamObserver<EventObject> responseObserver) {
        Timestamp timestamp = fromMillis(currentTimeMillis());
        EventObject.Builder e = EventObject.newBuilder()
                .setSource("OOS-handler")
                .setType("New seed")
                .setAssignee("Admin")
                .setState(EventObject.State.NEW)
                .setSeverity(Severity.INFO)
                .setLastUpdated(timestamp)
                .setCreated(timestamp);
        e.addActivityBuilder()
                .setComment("Dette er en kommentar")
                .setDescription("Hva er det siste som er blitt gjort med hendelsen")
                .setLastModified(timestamp)
                .setLastModifiedBy("En_annen bruker");
        e.addActivityBuilder()
                .setComment("Dette er en annen kommentar")
                .setDescription("Noe annet som ble gjort med hendelsen")
                .setLastModifiedBy("Admin")
                .setLastModified(timestamp);
        e.addDataBuilder()
                .setKey("uri").setValue("https://www.nyuri.no");
        e.addDataBuilder()
                .setKey("referring uri").setValue("https://www.funnether.no");
        responseObserver.onNext(e.build());
        responseObserver.onCompleted();
        //handleGet(() -> db.getEventObject(request), responseObserver);
    }

    @Override
    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
    public void listEventObjects(ListRequest request, StreamObserver<EventObject> responseObserver) {
        Timestamp timestamp = fromMillis(currentTimeMillis());
        //        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try (ChangeFeed<EventObject> c = db.listEventObjects(request);) {
//                    c.stream().forEach(o -> responseObserver.onNext(o));
//                    responseObserver.onCompleted();
//                } catch (Exception ex) {
//                    LOG.error(ex.getMessage(), ex);
//                    Status status = Status.UNKNOWN.withDescription(ex.toString());
//                    responseObserver.onError(status.asException());
//                }
//            }
//        }).start();
        EventObject.Builder e = EventObject.newBuilder()
                .setSource("OOS-handler")
                .setType("New seed")
                .setAssignee("")
                .setState(EventObject.State.NEW)
                .setSeverity(Severity.INFO)
                .setLastUpdated(timestamp)
                .setCreated(timestamp);
        e.addActivityBuilder()
                .setComment("")
                .setDescription("Hva er det siste som er blitt gjort med hendelsen")
                .setLastModified(timestamp)
                .setLastModifiedBy("En_annen bruker");
        e.addActivityBuilder()
                .setComment("Dette er en annen kommentar")
                .setDescription("Noe annet som ble gjort med hendelsen")
                .setLastModifiedBy("OOS-handler")
                .setLastModified(timestamp);
        e.addDataBuilder()
                .setKey("uri").setValue("https://www.nyuri2.no");
        e.addDataBuilder()
                .setKey("referring uri").setValue("https://www.funnether2.no");
        responseObserver.onNext(e.build());

        EventObject.Builder f = EventObject.newBuilder()
                .setSource("OOS-handler")
                .setType("New seed")
                .setAssignee("Andreas")
                .setState(EventObject.State.NEW)
                .setSeverity(Severity.INFO)
                .setLastUpdated(timestamp)
                .setCreated(timestamp);
        e.addActivityBuilder()
                .setComment("Dette er en kommentar")
                .setDescription("Oppdatert assignee")
                .setLastModified(timestamp)
                .setLastModifiedBy("Andreas");
        e.addActivityBuilder()
                .setComment("Dette er en annen kommentar")
                .setDescription("Noe annet som ble gjort med hendelsen")
                .setLastModifiedBy("Admin")
                .setLastModified(timestamp);
        e.addDataBuilder()
                .setKey("uri").setValue("https://www.nyuri3.no");
        e.addDataBuilder()
                .setKey("referring uri").setValue("https://www.funnether3.no");
        responseObserver.onNext(f.build());
        responseObserver.onCompleted();

    }

//    @Override
//    @AllowedRoles({Role.READONLY, Role.CURATOR, Role.ADMIN})
//    public void countEventObjects(ListRequest request, StreamObserver<ListCountResponse> responseObserver) {
//        handleGet(() -> db.countEventObjects(request), responseObserver);
//    }
//
//
//    @Override
//    @AllowedRoles({Role.CURATOR, Role.ADMIN})
//    public void saveEventObject(EventObject request, StreamObserver<EventObject> responseObserver) {
//        try {
//            responseObserver.onNext(db.saveEventObject(request));
//            responseObserver.onCompleted();
//        } catch (Exception ex) {
//            LOG.error(ex.getMessage(), ex);
//            Status status = Status.UNKNOWN.withDescription(ex.toString());
//            responseObserver.onError(status.asException());
//        }
//    }
//
//    @Override
//    @AllowedRoles({Role.CURATOR, Role.ADMIN})
//    public void updateEventObjects(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
//        try {
//            responseObserver.onNext(db.updateEventObjects(request));
//            responseObserver.onCompleted();
//        } catch (Exception ex) {
//            LOG.error(ex.getMessage(), ex);
//            Status status = Status.UNKNOWN.withDescription(ex.toString());
//            responseObserver.onError(status.asException());
//        }
//    }
//
//    @Override
//    @AllowedRoles({Role.ADMIN})
//    public void deleteEventObject(EventObject request, StreamObserver<DeleteResponse> responseObserver) {
//        try {
//            responseObserver.onNext(db.deleteEventObject(request));
//            responseObserver.onCompleted();
//        } catch (Exception ex) {
//            LOG.error(ex.getMessage(), ex);
//            Status status = Status.UNKNOWN.withDescription(ex.toString());
//            responseObserver.onError(status.asException());
//        }
//    }

}
