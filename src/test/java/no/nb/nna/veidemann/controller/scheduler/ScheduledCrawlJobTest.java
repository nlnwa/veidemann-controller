package no.nb.nna.veidemann.controller.scheduler;

import com.google.protobuf.Timestamp;
import no.nb.nna.veidemann.api.commons.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.BrowserConfig;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlConfig;
import no.nb.nna.veidemann.api.config.v1.CrawlLimitsConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.Seed;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionId;
import no.nb.nna.veidemann.api.frontier.v1.CrawlSeedRequest;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus.State;
import no.nb.nna.veidemann.api.report.v1.JobExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.db.ExecutionsAdapter;
import no.nb.nna.veidemann.commons.util.ApiTools;
import no.nb.nna.veidemann.controller.ArrayChangeFeed;
import no.nb.nna.veidemann.controller.FrontierClient;
import no.nb.nna.veidemann.controller.JobExecutionUtil;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ScheduledCrawlJobTest {
    private ConfigAdapter configAdapterMock;
    private ExecutionsAdapter executionsAdapterMock;
    private List<CrawlSeedRequest> frontierInvocations;

    private DbService db;

    @BeforeEach
    public void beforeEachTest() throws DbException {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        configAdapterMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(configAdapterMock);
        executionsAdapterMock = mock(ExecutionsAdapter.class);
        when(dbProviderMock.getExecutionsAdapter()).thenReturn(executionsAdapterMock);

        db = DbService.configure(dbProviderMock);
        FrontierClient frontierClientMock = mock(FrontierClient.class);
        JobExecutionUtil.addFrontierClient("url", frontierClientMock);
        frontierInvocations = Collections.synchronizedList(new ArrayList<>());
        when(frontierClientMock.crawlSeed(any(), any(), any(), any()))
                .thenAnswer(invocation -> {
                    try {
                        CrawlSeedRequest.Builder request = CrawlSeedRequest.newBuilder()
                                .setJob((ConfigObject) invocation.getArgument(0))
                                .setSeed((ConfigObject) invocation.getArgument(1))
                                .setJobExecutionId(((JobExecutionStatus) invocation.getArgument(2)).getId());
                        if (invocation.getArgument(3) != null) {
                            request.setTimeout(ProtoUtils.odtToTs(invocation.getArgument(3)));
                        }

                        frontierInvocations.add(request.build());
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                    return CrawlExecutionId.newBuilder().setId("ceid1").build();
                });

        ConfigObject entity = newConfObj(Kind.crawlEntity, "en1", ann("bs3", "bs3valFromEntity")).build();
        ConfigRef entityRef = ApiTools.refForConfig(entity);
        when(configAdapterMock.getConfigObject(entityRef))
                .thenReturn(entity);

        ConfigObject scopescript = newConfObj(Kind.browserScript, "scp", ann("scp", "scpvalFromScript")).build();
        ConfigRef scopescriptRef = ApiTools.refForConfig(scopescript);
        when(configAdapterMock.getConfigObject(scopescriptRef))
                .thenReturn(scopescript);

        ConfigObject browserConfig = newConfObj(Kind.browserConfig, "bc")
                .setBrowserConfig(BrowserConfig.newBuilder())
                .build();
        ConfigRef browserConfigRef = ApiTools.refForConfig(browserConfig);
        when(configAdapterMock.getConfigObject(browserConfigRef))
                .thenReturn(browserConfig);

        ConfigObject crawlConfig = newConfObj(Kind.crawlConfig, "cc")
                .setCrawlConfig(CrawlConfig.newBuilder().setBrowserConfigRef(browserConfigRef))
                .build();
        ConfigRef crawlConfigRef = ApiTools.refForConfig(crawlConfig);
        when(configAdapterMock.getConfigObject(crawlConfigRef))
                .thenReturn(crawlConfig);

        ConfigObject.Builder jobConfigB = newConfObj(Kind.crawlJob, "job1", ann("scp", "scpvalFromJob"));
        jobConfigB.getCrawlJobBuilder()
                .setCrawlConfigRef(crawlConfigRef)
                .setScopeScriptRef(scopescriptRef);
        ConfigObject jobConfig1 = jobConfigB.build();
        ConfigRef jobConfig1Ref = ApiTools.refForConfig(jobConfig1);
        when(configAdapterMock.getConfigObject(jobConfig1Ref))
                .thenReturn(jobConfig1);

        jobConfigB = newConfObj(Kind.crawlJob, "job2");
        jobConfigB.getCrawlJobBuilder()
                .setCrawlConfigRef(crawlConfigRef)
                .setScopeScriptRef(scopescriptRef);
        ConfigObject jobConfig2 = jobConfigB.build();
        ConfigRef jobConfig2Ref = ApiTools.refForConfig(jobConfig2);
        when(configAdapterMock.getConfigObject(jobConfig2Ref))
                .thenReturn(jobConfig2);

        jobConfigB = newConfObj(Kind.crawlJob, "job3");
        jobConfigB.getCrawlJobBuilder()
                .setLimits(CrawlLimitsConfig.newBuilder().setMaxDurationS(10))
                .setCrawlConfigRef(crawlConfigRef)
                .setScopeScriptRef(scopescriptRef);
        ConfigObject jobConfig3 = jobConfigB.build();
        ConfigRef jobConfig3Ref = ApiTools.refForConfig(jobConfig3);
        when(configAdapterMock.getConfigObject(jobConfig3Ref))
                .thenReturn(jobConfig3);

        ConfigObject.Builder seedB = newConfObj(Kind.seed, "seed1", ann("bs1", "bs1valFromSeed"), ann("{job2}bs2", "bs2valFromSeed"));
        seedB.getSeedBuilder()
                .setEntityRef(entityRef)
                .addJobRef(jobConfig1Ref)
                .addJobRef(jobConfig3Ref);
        ConfigObject seed1 = seedB.build();
        ConfigRef seed1Ref = ApiTools.refForConfig(seed1);
        when(configAdapterMock.getConfigObject(seed1Ref))
                .thenReturn(seed1);

        seedB = newConfObj(Kind.seed, "seed2", ann("bs1", "bs1valFromSeed"), ann("{job1}bs2", "bs2valFromSeed"));
        seedB.getSeedBuilder()
                .setEntityRef(entityRef)
                .addJobRef(jobConfig1Ref);
        ConfigObject seed2 = seedB.build();
        ConfigRef seed2Ref = ApiTools.refForConfig(seed2);
        when(configAdapterMock.getConfigObject(seed2Ref))
                .thenReturn(seed2);

        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder().setKind(Kind.browserScript).build()))
                .thenReturn(new ArrayChangeFeed<>());
        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setQueryTemplate(ConfigObject.newBuilder().setSeed(Seed.newBuilder().addJobRef(jobConfig1Ref)))
                .setQueryMask(FieldMask.newBuilder().addPaths("seed.jobRef"))
                .build()))
                .thenReturn(new ArrayChangeFeed<>(seed1, seed2));
        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setQueryTemplate(ConfigObject.newBuilder().setSeed(Seed.newBuilder().addJobRef(jobConfig2Ref)))
                .setQueryMask(FieldMask.newBuilder().addPaths("seed.jobRef"))
                .build()))
                .thenReturn(new ArrayChangeFeed<>());
        when(configAdapterMock.listConfigObjects(ListRequest.newBuilder()
                .setKind(Kind.seed)
                .setQueryTemplate(ConfigObject.newBuilder().setSeed(Seed.newBuilder().addJobRef(jobConfig3Ref)))
                .setQueryMask(FieldMask.newBuilder().addPaths("seed.jobRef"))
                .build()))
                .thenReturn(new ArrayChangeFeed<>(seed1));

        Hashtable<String, JobExecutionStatus> running = new Hashtable<>(3);
        when(executionsAdapterMock.listJobExecutionStatus(any()))
                .thenAnswer(invocation -> {
                    JobExecutionsListRequest r = invocation.getArgument(0);
                    if (r.getStateList().contains(State.RUNNING)) {
                        JobExecutionStatus jes = running.get(r.getQueryTemplate().getJobId());
                        if (jes != null && jes.getState() == State.RUNNING) {
                            return new ArrayChangeFeed<JobExecutionStatus>(jes);
                        }
                    }
                    return new ArrayChangeFeed<>();
                });

        when(executionsAdapterMock.createJobExecutionStatus(anyString()))
                .then(i -> {
                    Thread.sleep(100);
                    running.put(i.getArgument(0), JobExecutionStatus.newBuilder().setId(i.getArgument(0) + "Execution1").setState(State.RUNNING).build());
                    return running.get(i.getArgument(0));
                });
    }

    @AfterEach
    public void afterEachTest() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void runScheduledCrawl() throws DbException, InterruptedException {
        // Test crawl all seeds for a job
        ScheduledCrawlJob scj = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job1").build()));
        scj.execute(null);

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(1000);
        assertThat(frontierInvocations).hasSize(2);

        Collections.sort(frontierInvocations, (o1, o2) -> o1.getSeed().getId().compareTo(o2.getSeed().getId()));
        assertThat(frontierInvocations.get(0).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(0).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(0).getTimeout()).isInstanceOf(Timestamp.class);
        assertThat(frontierInvocations.get(1).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(1).getSeed().getId()).isEqualTo("seed2");
        assertThat(frontierInvocations.get(1).getTimeout()).isInstanceOf(Timestamp.class);

        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job3");
    }

    @Test
    void runScheduledCrawlWithNoSeeds() throws DbException, InterruptedException {
        // Test crawl all seeds for a job
        ScheduledCrawlJob scj = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job2").build()));
        scj.execute(null);

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(1000);
        assertThat(frontierInvocations).hasSize(0);

        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job3");
    }

    @Test
    void runTwoScheduledCrawlsFromDifferentJobs() throws DbException, InterruptedException {
        // Test crawl all seeds for a job
        ScheduledCrawlJob scj1 = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job1").build()));
        ScheduledCrawlJob scj2 = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job3").build()));
        scj1.execute(null);
        scj2.execute(null);

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(1000);
        assertThat(frontierInvocations).hasSize(3);

        Collections.sort(frontierInvocations, (o1, o2) -> o1.getJob().getId().compareTo(o2.getJob().getId()) + o1.getSeed().getId().compareTo(o2.getSeed().getId()));
        assertThat(frontierInvocations.get(0).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(0).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(0).getTimeout()).isInstanceOf(Timestamp.class);
        assertThat(frontierInvocations.get(1).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(1).getSeed().getId()).isEqualTo("seed2");
        assertThat(frontierInvocations.get(1).getTimeout()).isInstanceOf(Timestamp.class);
        assertThat(frontierInvocations.get(2).getJob().getId()).isEqualTo("job3");
        assertThat(frontierInvocations.get(2).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(2).getTimeout()).isInstanceOf(Timestamp.class);

        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job3");
    }

    @Test
    void runTwoScheduledCrawlsFromSameJob() throws DbException, InterruptedException {
        // Test crawl all seeds for a job
        ScheduledCrawlJob scj1 = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job1").build()));
        ScheduledCrawlJob scj2 = new ScheduledCrawlJob(DbService.getInstance().getConfigAdapter()
                .getConfigObject(ConfigRef.newBuilder().setKind(Kind.crawlJob).setId("job1").build()));
        ForkJoinPool.commonPool().execute(() -> scj1.execute(null));
        ForkJoinPool.commonPool().execute(() -> scj2.execute(null));

        // Let async submission of Frontier requests get a little time to finish.
        Thread.sleep(1000);
        assertThat(frontierInvocations).hasSize(2);

        Collections.sort(frontierInvocations, (o1, o2) -> o1.getSeed().getId().compareTo(o2.getSeed().getId()));
        assertThat(frontierInvocations.get(0).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(0).getSeed().getId()).isEqualTo("seed1");
        assertThat(frontierInvocations.get(0).getTimeout()).isInstanceOf(Timestamp.class);
        assertThat(frontierInvocations.get(1).getJob().getId()).isEqualTo("job1");
        assertThat(frontierInvocations.get(1).getSeed().getId()).isEqualTo("seed2");
        assertThat(frontierInvocations.get(1).getTimeout()).isInstanceOf(Timestamp.class);

        verify(executionsAdapterMock, times(1)).createJobExecutionStatus("job1");
        verify(executionsAdapterMock, times(0)).createJobExecutionStatus("job2");
    }

    ////////////////////
    // Helper methods //
    ////////////////////

    private ConfigObject.Builder newConfObj(Kind kind, String name, Annotation... annotations) {
        ConfigObject.Builder co = ConfigObject.newBuilder().setKind(kind).setId(name);
        co.getMetaBuilder().setName(name);
        for (Annotation a : annotations) {
            co.getMetaBuilder().addAnnotation(a);
        }
        return co;
    }

    private Annotation ann(String key, String value) {
        return Annotation.newBuilder().setKey(key).setValue(value).build();
    }

}
