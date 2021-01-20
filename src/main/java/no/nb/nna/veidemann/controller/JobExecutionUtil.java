package no.nb.nna.veidemann.controller;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.controller.v1.CrawlerStatus;
import no.nb.nna.veidemann.api.frontier.v1.CountResponse;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlHostGroup;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.controller.ControllerApiServer.JobExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static no.nb.nna.veidemann.commons.util.ApiTools.buildLabel;

public class JobExecutionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionUtil.class);

    public final static String SEED_TYPE_LABEL_KEY = "v7n_seed-type";
    private final static Map<String, FrontierClient> frontierClients = new HashMap<>();

    private final static ExecutorService exe = Executors.newFixedThreadPool(16);
    private final static ExecutorService submitSeedExecutor =
            new ThreadPoolExecutor(4, 16, 10L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue(5000), new CallerRunsPolicy());

    private JobExecutionUtil() {
    }

    public static void addFrontierClient(String seedType, FrontierClient client) {
        frontierClients.put(seedType.toLowerCase(), client);
    }

    /**
     * Helper method for getting one object. Sends NOT_FOUND if responseSupplier returns null.
     *
     * @param responseSupplier the supplier which result is checked for null
     * @param responseObserver the observer to send the object to
     */
    public static <T> void handleGet(CheckedSupplier<T, DbException> responseSupplier, StreamObserver<T> responseObserver) {
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

    public static boolean crawlSeed(CompletionService<CrawlExecutionId> submitSeedCompletionService, ConfigObject job, ConfigObject seed, JobExecutionStatus jobExecutionStatus,
                                    Map<String, Annotation> jobAnnotations, OffsetDateTime timeout, boolean addToRunningJob) {
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

            String type = ApiTools.getFirstLabelWithKey(seed.getMeta(), SEED_TYPE_LABEL_KEY)
                    .orElse(buildLabel(SEED_TYPE_LABEL_KEY, "url")).getValue().toLowerCase();

            FrontierClient frontierClient = frontierClients.get(type);

            if (frontierClient != null) {
                if (submitSeedCompletionService == null) {
                    try {
                        frontierClient.crawlSeed(job, seed, jobExecutionStatus, timeout);
                        return true;
                    } catch (Exception e) {
                        LOG.warn("Unable to submit seed '{}' for crawling", seed.getMeta().getName(), e);
                        return false;
                    }
                } else {
                    submitSeedCompletionService.submit(() -> frontierClient.crawlSeed(job, seed, jobExecutionStatus, timeout));
                    return true;
                }
            } else {
                LOG.warn("No frontier defined for seed type {}", type);
                return false;
            }
        }
        LOG.debug("Seed '{}' is disabled", seed.getMeta().getName());
        return false;
    }

    public static JobExecutionStatus submitSeeds(ConfigObject job, JobExecutionStatus jobExecutionStatus, OffsetDateTime timeout,
                                                 boolean addToRunningJob, List<JobExecutionListener> jobExecutionListeners) {
        SettableFuture<JobExecutionStatus> jesFuture = SettableFuture.create();

        ListRequest.Builder seedRequest = ListRequest.newBuilder()
                .setKind(Kind.seed);
        seedRequest.getQueryMaskBuilder().addPaths(Kind.seed.name() + ".jobRef");
        seedRequest.getQueryTemplateBuilder().getSeedBuilder().addJobRefBuilder().setKind(Kind.crawlJob).setId(job.getId());

        exe.submit(() -> {
            AtomicLong count = new AtomicLong(0L);
            AtomicLong rejected = new AtomicLong(0L);
            AtomicBoolean done = new AtomicBoolean(false);

            try (ChangeFeed<ConfigObject> seeds = DbService.getInstance().getConfigAdapter().listConfigObjects(seedRequest.build())) {
                Iterator<ConfigObject> it = seeds.stream().iterator();
                if (it.hasNext()) {
                    Map<String, Annotation> jobAnnotations = JobExecutionUtil.GetScriptAnnotationsForJob(job);

                    JobExecutionStatus jes = createJobExecutionStatusIfNotExist(job, jobExecutionStatus);
                    jesFuture.set(jes);

                    CompletionService<CrawlExecutionId> completionService = new ExecutorCompletionService<>(submitSeedExecutor, new LinkedBlockingQueue<>());
                    exe.execute(() -> {
                        for (JobExecutionListener l : jobExecutionListeners) {
                            l.onJobStarting(jes.getId());
                        }

                        AtomicLong failed = new AtomicLong(0L);
                        AtomicLong processed = new AtomicLong(0);
                        while (done.get() == false || processed.get() < count.get()) {
                            CrawlExecutionId ceid = null;
                            try {
                                Future<CrawlExecutionId> f = completionService.take();
                                processed.incrementAndGet();
                                ceid = f.get();
                                LOG.trace("Crawl Execution '{}' created", ceid.getId());
                            } catch (Exception e) {
                                failed.incrementAndGet();
                                LOG.info("Error starting crawl of seed: " + e.getMessage(), e);
                            }
                        }
                        LOG.info("{} seeds of {} for job '{}' rejected by frontier", failed.get(), count.get(), job.getMeta().getName());
                        for (JobExecutionListener l : jobExecutionListeners) {
                            l.onJobStarted(jes.getId());
                        }
                    });

                    it.forEachRemaining(seed -> {
                        if (crawlSeed(completionService, job, seed, jes, jobAnnotations, timeout, addToRunningJob)) {
                            count.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    });
                    done.set(true);
                    LOG.info("{} seeds for job '{}' submitted, {} seeds rejected", count.get(), job.getMeta().getName(), rejected.get());
                } else {
                    jesFuture.set(null);
                    LOG.info("No seeds for job '{}'", job.getMeta().getName());
                }
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            return jesFuture.get();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Gets a running jobExecution for a job or null if not running.
     *
     * @param job the job to check if running
     * @return a JobExecutionStatus with state RUNNING or null if no job was not running
     * @throws DbException
     */
    public static JobExecutionStatus getRunningJobExecutionStatusForJob(ConfigObject job) throws DbException {
        ChangeFeed<JobExecutionStatus> runningJobsR = DbService.getInstance().getExecutionsAdapter()
                .listJobExecutionStatus(JobExecutionsListRequest.newBuilder()
                        .setQueryMask(FieldMask.newBuilder().addPaths("jobId"))
                        .setQueryTemplate(JobExecutionStatus.newBuilder().setJobId(job.getId()))
                        .addState(State.RUNNING).build());

        return runningJobsR.stream().findAny().orElse(null);
    }

    public static JobExecutionStatus createJobExecutionStatusIfNotExist(ConfigObject job, JobExecutionStatus existingJobExecutionStatus) throws DbException {
        if (existingJobExecutionStatus == null) {
            JobExecutionStatus jes = DbService.getInstance().getExecutionsAdapter()
                    .createJobExecutionStatus(job.getId());
            LOG.info("Creating new job execution '{}'", jes.getId());
            return jes;
        } else {
            return existingJobExecutionStatus;
        }
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

    public static Map<String, Annotation> GetScriptAnnotationsForJob(ConfigObject jobConfig) throws DbException {
        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        ConfigObject crawlConfig = db.getConfigObject(jobConfig.getCrawlJob().getCrawlConfigRef());
        ConfigObject browserConfig = db.getConfigObject(crawlConfig.getCrawlConfig().getBrowserConfigRef());

        Map<String, Annotation> annotations = new HashMap<>();

        // Get scope script annotations
        if (!jobConfig.getCrawlJob().hasScopeScriptRef()) {
            throw new IllegalArgumentException("Missing scopescript ref for crawl job " + jobConfig.getId());
        }
        db.getConfigObject(jobConfig.getCrawlJob().getScopeScriptRef()).getMeta().getAnnotationList()
                .forEach(a -> annotations.put(a.getKey(), a));

        // Get annotations for referenced browser scripts
        browserConfig.getBrowserConfig().getScriptRefList().forEach(r -> {
            try {
                db.getConfigObject(r).getMeta().getAnnotationList().forEach(a -> annotations.put(a.getKey(), a));
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        });

        // Get annotations for browser scripts matching selectors
        db.listConfigObjects(ListRequest.newBuilder().setKind(Kind.browserScript).addAllLabelSelector(
                browserConfig.getBrowserConfig().getScriptSelectorList()).build()).stream()
                .flatMap(s -> s.getMeta().getAnnotationList().stream())
                .forEach(a -> annotations.put(a.getKey(), a));

        // Override with job specific annotations
        jobConfig.getMeta().getAnnotationList().stream()
                .filter(a -> annotations.containsKey(a.getKey()))
                .forEach(a -> annotations.put(a.getKey(), a));

        return Collections.unmodifiableMap(annotations);
    }

    public static Map<String, Annotation> GetScriptAnnotationOverridesForSeed(
            ConfigObject seed, ConfigObject jobConfig, Map<String, Annotation> annotations) throws DbException {

        ConfigAdapter db = DbService.getInstance().getConfigAdapter();

        Map<String, Annotation> result = new HashMap<>();
        result.putAll(annotations);

        if (seed.getSeed().hasEntityRef()) {
            overrideAnnotation(db.getConfigObject(seed.getSeed().getEntityRef()).getMeta().getAnnotationList(), jobConfig, result);
        }

        overrideAnnotation(seed.getMeta().getAnnotationList(), jobConfig, result);

        return result;
    }

    static void overrideAnnotation(List<Annotation> annotations, ConfigObject jobConfig, Map<String, Annotation> jobAnnotations) {
        List<Annotation> ann = new ArrayList<>();
        ann.addAll(annotations);
        for (Iterator<Annotation> it = ann.iterator(); it.hasNext(); ) {
            Annotation a = it.next();
            if (jobAnnotations.containsKey(a.getKey())) {
                jobAnnotations.put(a.getKey(), a);
                it.remove();
            }
        }
        for (Annotation a : ann) {
            if (a.getKey().startsWith("{")) {
                int endIdx = a.getKey().indexOf('}');
                if (endIdx == -1) {
                    throw new IllegalArgumentException("Missing matching '}' for annotation: " + a.getKey());
                }
                String jobIdOrName = a.getKey().substring(1, endIdx);
                String key = a.getKey().substring(endIdx + 1);
                if ((jobConfig.getId().equals(jobIdOrName) || jobConfig.getMeta().getName().equals(jobIdOrName))
                        && jobAnnotations.containsKey(key)) {
                    a = a.toBuilder().setKey(key).build();
                    jobAnnotations.put(a.getKey(), a);
                }
            }
        }
    }

    @FunctionalInterface
    public interface CheckedSupplier<T, E extends Exception> {
        T get() throws E;
    }
}
