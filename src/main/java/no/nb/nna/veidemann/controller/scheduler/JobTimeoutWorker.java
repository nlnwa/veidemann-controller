package no.nb.nna.veidemann.controller.scheduler;

import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.ChangeFeed;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JobTimeoutWorker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(JobTimeoutWorker.class);

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);

    public JobTimeoutWorker() {
        initExistingJobs();
    }

    public ScheduledFuture addJobExecution(String id, long timeoutSeconds) {
        if (timeoutSeconds <= 0) {
            return null;
        }

        return executorService.schedule(() -> {
            try {
                DbService.getInstance().getExecutionsAdapter().setJobExecutionStateAbortedTimeout(id);
            } catch (DbException e) {
                LOG.error("Failed aborting job: {}", e.getMessage(), e);
            }
        }, timeoutSeconds, TimeUnit.SECONDS);
    }

    private void initExistingJobs() {
        ChangeFeed<JobExecutionStatus> runningJobsR = null;
        try {
            runningJobsR = DbService.getInstance().getExecutionsAdapter()
                    .listJobExecutionStatus(JobExecutionsListRequest.newBuilder()
                            .addState(State.CREATED)
                            .addState(State.RUNNING)
                            .build());
        } catch (DbException e) {
            LOG.error("Failed getting running jobs: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
        runningJobsR.stream().forEach(jes -> {
            String jobId = jes.getJobId();
            String jobExecutionId = jes.getId();
            long maxDurationS = 0;
            try {
                maxDurationS = DbService.getInstance().getConfigAdapter()
                        .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId(jobId).build())
                        .getCrawlJob().getLimits().getMaxDurationS();
            } catch (DbException e) {
                LOG.error("Failed getting job config: {}", e.getMessage(), e);
            }
            addJobExecution(jobExecutionId, maxDurationS);
        });
    }

    @Override
    public void close() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.out.println("JobTimeoutWorker was interrupted while shutting down");
        }
    }
}
