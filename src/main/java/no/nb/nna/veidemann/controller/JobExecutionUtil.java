package no.nb.nna.veidemann.controller;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.controller.v1.CrawlerStatus;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
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

    public static boolean crawlSeed(ConfigObject job, ConfigObject seed, JobExecutionStatus jobExecutionStatus,
                                    OffsetDateTime timeout, boolean addToRunningJob) {
        if (!seed.getSeed().getDisabled()) {

            if (addToRunningJob) {
                CrawlExecutionsListRequest.Builder req = CrawlExecutionsListRequest.newBuilder();
                req.getQueryTemplateBuilder()
                        .setSeedId(seed.getId())
                        .setJobExecutionId(jobExecutionStatus.getId());
                req.getQueryMaskBuilder()
                        .addPaths("seedId")
                        .addPaths("jobExecutionId");
                try {
                    if (DbService.getInstance().getExecutionsAdapter().listCrawlExecutionStatus(req.build()).stream().findAny().isPresent()) {
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
                    frontierClient.crawlSeed(job, seed, jobExecutionStatus, timeout);
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

    public static void submitSeeds(ConfigObject job, JobExecutionStatus jobExecutionStatus, OffsetDateTime timeout,
                                   boolean addToRunningJob) {
        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        ListRequest.Builder seedRequest = ListRequest.newBuilder().setKind(Kind.seed);
        seedRequest.getQueryMaskBuilder().addPaths(Kind.seed.name() + ".jobRef");
        seedRequest.getQueryTemplateBuilder().getSeedBuilder().addJobRefBuilder().setKind(Kind.crawlJob).setId(job.getId());

        exe.submit(() -> {
            AtomicLong count = new AtomicLong(0L);
            AtomicLong rejected = new AtomicLong(0L);
            try (ChangeFeed<ConfigObject> seeds = db.listConfigObjects(seedRequest.build())) {
                seeds.stream().forEach(s -> {
                    if (crawlSeed(job, s, jobExecutionStatus, timeout, addToRunningJob)) {
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

    public static void queueCountAndBusyChgCount(FutureCallback<CrawlerStatus.Builder> callback) {
        String type = "url";
        FrontierClient frontierClient = frontierClients.get(type);

        if (frontierClient != null) {
            ListenableFuture<CountResponse> queueCount = frontierClient.queueCountTotal();
            ListenableFuture<CountResponse> chgCount = frontierClient.busyCrawlHostGroupCount();
            ListenableFuture<CrawlerStatus.Builder> queueAndChgCount = Futures.whenAllSucceed(queueCount, chgCount)
                    .call(
                            () -> CrawlerStatus.newBuilder()
                                    .setBusyCrawlHostGroupCount(Futures.getDone(chgCount).getCount())
                                    .setQueueSize(Futures.getDone(queueCount).getCount()),
                            exe);
            Futures.addCallback(queueAndChgCount, callback, exe);
        } else {
            LOG.warn("No frontier defined for seed type {}", type);
            callback.onFailure(new IllegalArgumentException("No frontier defined for seed type " + type));
        }
    }

    public static void queueCountForCrawlExecution(CrawlExecutionId crawlExecutionId, FutureCallback<CountResponse> callback) {
        String type = "url";
        FrontierClient frontierClient = frontierClients.get(type);

        if (frontierClient != null) {
            frontierClient.queueCountForCrawlExecution(crawlExecutionId, callback, exe);
        } else {
            LOG.warn("No frontier defined for seed type {}", type);
            callback.onFailure(new IllegalArgumentException("No frontier defined for seed type " + type));
        }
    }

    public static void queueCountForCrawlHostGroup(CrawlHostGroup crawlHostGroup, FutureCallback<CountResponse> callback) {
        String type = "url";
        FrontierClient frontierClient = frontierClients.get(type);

        if (frontierClient != null) {
            frontierClient.queueCountForCrawlHostGroup(crawlHostGroup, callback, exe);
        } else {
            LOG.warn("No frontier defined for seed type {}", type);
            callback.onFailure(new IllegalArgumentException("No frontier defined for seed type " + type));
        }
    }

    public static OffsetDateTime calculateTimeout(ConfigObject job) {
        OffsetDateTime timeout = null;
        if (job.getCrawlJob().hasLimits()) {
            long maxDurationS = job.getCrawlJob().getLimits().getMaxDurationS();
            if (maxDurationS > 0) {
                timeout = OffsetDateTime.now().plus(maxDurationS, ChronoUnit.SECONDS);
            }
        }
        return timeout;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
