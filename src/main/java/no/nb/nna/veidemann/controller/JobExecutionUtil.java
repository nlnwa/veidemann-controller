package no.nb.nna.veidemann.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    public static boolean crawlSeed(ConfigObject job, ConfigObject seed, JobExecutionStatus jobExecutionStatus, boolean addToRunningJob) {
        if (!seed.getSeed().getDisabled()) {

            if (addToRunningJob) {
                ListExecutionsRequest.Builder req = ListExecutionsRequest.newBuilder()
                        .setSeedId(seed.getId())
                        .setJobExecutionId(jobExecutionStatus.getId());
                try {
                    if (DbService.getInstance().getExecutionsAdapter().listCrawlExecutionStatus(req.build()).getCount() > 0) {
                        LOG.debug("Seed '{}' is already crawling for jobExecution {}", seed.getMeta().getName(), jobExecutionStatus.getId());
                        return false;
                    }
                } catch (DbException e) {
                    LOG.warn("Error crawling seed '{}'", seed.getMeta().getName(), e);
                    return false;
                }
            }

            LOG.debug("Start harvest of: {}", seed.getMeta().getName());

            String type = ApiTools.getFirstLabelWithKey(seed.getMeta(), "type")
                    .orElse(buildLabel("type", "url")).getValue().toLowerCase();

            FrontierClient frontierClient = frontierClients.get(type);

            if (frontierClient != null) {
                exe.submit(() -> {
                    frontierClient.crawlSeed(job, seed, jobExecutionStatus);
                });
            } else {
                LOG.warn("No frontier defined for seed type {}", type);
                return false;
            }
            return true;
        }
        LOG.debug("Seed '{}' is disabled", seed.getMeta().getName());
        return false;
    }

    public static void submitSeeds(ConfigObject job, JobExecutionStatus jobExecutionStatus, boolean addToRunningJob) {
        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        ListRequest.Builder seedRequest = ListRequest.newBuilder().setKind(Kind.seed);
        seedRequest.getQueryMaskBuilder().addPaths(Kind.seed.name() + ".jobRef");
        seedRequest.getQueryTemplateBuilder().getSeedBuilder().addJobRefBuilder().setKind(Kind.crawlJob).setId(job.getId());

        exe.submit(() -> {
            AtomicLong count = new AtomicLong(0L);
            AtomicLong rejected = new AtomicLong(0L);
            try (ChangeFeed<ConfigObject> seeds = db.listConfigObjects(seedRequest.build())) {
                seeds.stream().forEach(s -> {
                    if (crawlSeed(job, s, jobExecutionStatus, addToRunningJob)) {
                        count.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                });
            } catch (Exception e) {
                LOG.error("Error while submitting seeds: " + e.getMessage(), e);
            }
            LOG.info("{} seeds for job '{}' started, {} seeds rejected", count.get(), job.getMeta().getName(), rejected.get());
        });
    }

    @FunctionalInterface
    public interface CheckedSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
