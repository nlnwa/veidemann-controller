package no.nb.nna.veidemann.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ControllerProto.RunCrawlReply;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static no.nb.nna.veidemann.commons.util.ApiTools.buildLabel;

public class JobExecutionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionUtil.class);

    private final static Map<String, FrontierClient> frontierClients = new HashMap<>();

    private final static ExecutorService exe = new ThreadPoolExecutor(0, 64, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue());

    private JobExecutionUtil() {
    }

    static void addFrontierClient(String seedType, FrontierClient client) {
        frontierClients.put(seedType.toLowerCase(), client);
    }

    /**
     * Helper method for getting one object. Sends NOT_FOUND if responseSupplier returns null.
     *
     * @param responseSupplier the supplier which result is checked for null
     * @param responseObserver the observer to send the object to
     */
    public static <T> void handleGet(CheckedSupplier<T, DbException> responseSupplier, StreamObserver responseObserver) {
        try {
            T response = responseSupplier.get();
            if (response == null) {
                Status status = Status.NOT_FOUND;
                responseObserver.onError(status.asException());
            } else {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    public static void crawlSeed(ConfigObject job, ConfigObject seed, JobExecutionStatus jobExecutionStatus) {
        if (!seed.getSeed().getDisabled()) {
            LOG.info("Start harvest of: {}", seed.getMeta().getName());

            String type = ApiTools.getFirstLabelWithKey(seed.getMeta(), "type")
                    .orElse(buildLabel("type", "url")).getValue().toLowerCase();

            FrontierClient frontierClient = frontierClients.get(type);

            if (frontierClient != null) {
                exe.submit(() -> {
                    frontierClient.crawlSeed(job, seed, jobExecutionStatus);
                });
            } else {
                LOG.warn("No frontier defined for seed type {}", type);
            }
        }
    }

    public static void submitSeeds(ConfigObject job, JobExecutionStatus jobExecutionStatus) {
        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        no.nb.nna.veidemann.api.config.v1.ListRequest.Builder seedRequest = no.nb.nna.veidemann.api.config.v1.ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setPageSize(Integer.MAX_VALUE);
        seedRequest.getQueryMaskBuilder().addPaths(Kind.seed.name() + ".jobRef");
        seedRequest.getQueryTemplateBuilder().getSeedBuilder().addJobRefBuilder().setKind(Kind.crawlJob).setId(job.getId());

        exe.submit(() -> {
            try (ChangeFeed<ConfigObject> seeds = db.listConfigObjects(seedRequest.build())) {
                seeds.stream().forEach(s -> crawlSeed(job, s, jobExecutionStatus));
            } catch (DbException e) {
                LOG.error("Error while submitting seeds: " + e.getMessage(), e);
            }
            LOG.info("All seeds for job '{}' started", job.getMeta().getName());
        });
    }

    @FunctionalInterface
    public interface CheckedSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
