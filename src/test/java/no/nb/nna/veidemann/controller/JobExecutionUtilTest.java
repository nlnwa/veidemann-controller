package no.nb.nna.veidemann.controller;

import com.google.common.collect.ImmutableMap;
import no.nb.nna.veidemann.api.config.v1.Annotation;
import no.nb.nna.veidemann.api.config.v1.BrowserConfig;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.CrawlConfig;
import no.nb.nna.veidemann.api.config.v1.Kind;
import no.nb.nna.veidemann.api.config.v1.ListRequest;
import no.nb.nna.veidemann.api.config.v1.Meta;
import no.nb.nna.veidemann.commons.db.ConfigAdapter;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.db.DbServiceSPI;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobExecutionUtilTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void getScriptAnnotationsForJob() throws DbException {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        ConfigAdapter dbMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(dbMock);
        try (DbService db = DbService.configure(dbProviderMock)) {

            ConfigObject script1 = newConfObj(Kind.browserScript, "bs1", ann("bs1", "bs1val1")).build();
            ConfigRef script1Ref = ApiTools.refForConfig(script1);
            when(dbMock.getConfigObject(script1Ref))
                    .thenReturn(script1);

            ConfigObject script2 = newConfObj(Kind.browserScript, "bs2", ann("bs2", "bs2val1")).build();
            ConfigRef script2Ref = ApiTools.refForConfig(script2);
            when(dbMock.getConfigObject(script2Ref))
                    .thenReturn(script2);

            ConfigObject script3 = newConfObj(Kind.browserScript, "bs3", ann("bs3", "bs3val1")).build();
            ConfigRef script3Ref = ApiTools.refForConfig(script3);
            when(dbMock.getConfigObject(script3Ref))
                    .thenReturn(script3);

            ConfigObject scopescript = newConfObj(Kind.browserScript, "scp", ann("scp", "scpval1")).build();
            ConfigRef scopescriptRef = ApiTools.refForConfig(scopescript);
            when(dbMock.getConfigObject(scopescriptRef))
                    .thenReturn(scopescript);

            ConfigObject browserConfig = newConfObj(Kind.browserConfig, "bc")
                    .setBrowserConfig(BrowserConfig.newBuilder().addScriptRef(script1Ref).addScriptSelector("scope:default"))
                    .build();
            ConfigRef browserConfigRef = ApiTools.refForConfig(browserConfig);
            when(dbMock.getConfigObject(browserConfigRef))
                    .thenReturn(browserConfig);

            ConfigObject crawlConfig = newConfObj(Kind.crawlConfig, "cc")
                    .setCrawlConfig(CrawlConfig.newBuilder().setBrowserConfigRef(browserConfigRef))
                    .build();
            ConfigRef crawlConfigRef = ApiTools.refForConfig(crawlConfig);
            when(dbMock.getConfigObject(crawlConfigRef))
                    .thenReturn(crawlConfig);

            when(dbMock.listConfigObjects(ListRequest.newBuilder().setKind(Kind.browserScript).addLabelSelector("scope:default").build()))
                    .thenReturn(new ArrayChangeFeed<>(script2, script3));

            ConfigObject.Builder jobConfig = newConfObj(Kind.crawlJob, "job1", ann("bs1", "bs1val2"));
            jobConfig.getCrawlJobBuilder()
                    .setCrawlConfigRef(crawlConfigRef)
                    .setScopeScript(scopescriptRef);

            Annotation expected1 = Annotation.newBuilder().setKey("bs1").setValue("bs1val2").build();
            Annotation expected2 = Annotation.newBuilder().setKey("bs2").setValue("bs2val1").build();
            Annotation expected3 = Annotation.newBuilder().setKey("bs3").setValue("bs3val1").build();
            Annotation expected4 = Annotation.newBuilder().setKey("scp").setValue("scpval1").build();

            assertThat(JobExecutionUtil.GetScriptAnnotationsForJob(jobConfig.build()))
                    .hasSize(4)
                    .containsOnlyKeys("bs1", "bs2", "bs3", "scp")
                    .containsValues(expected1, expected2, expected3, expected4);
        }
    }

    @Test
    void getScriptAnnotationOverridesForSeed() throws DbException {
        DbServiceSPI dbProviderMock = mock(DbServiceSPI.class);
        ConfigAdapter dbMock = mock(ConfigAdapter.class);
        when(dbProviderMock.getConfigAdapter()).thenReturn(dbMock);
        try (DbService db = DbService.configure(dbProviderMock)) {

            ConfigObject entity = newConfObj(Kind.browserScript, "en1", ann("bs3", "bs3val2")).build();
            ConfigRef entityRef = ApiTools.refForConfig(entity);
            when(dbMock.getConfigObject(entityRef))
                    .thenReturn(entity);

            ConfigObject.Builder seed = newConfObj(Kind.seed, "seed1", ann("bs1", "bs1val2"), ann("{job2}bs2", "bs2val2"));
            seed.getSeedBuilder()
                    .setEntityRef(entityRef);

            ConfigObject.Builder jobConfig = newConfObj(Kind.crawlJob, "job1");

            Annotation fromJob1 = Annotation.newBuilder().setKey("bs1").setValue("bs1val1").build();
            Annotation fromJob2 = Annotation.newBuilder().setKey("bs2").setValue("bs2val1").build();
            Annotation fromJob3 = Annotation.newBuilder().setKey("bs3").setValue("bs3val1").build();
            Annotation fromJob4 = Annotation.newBuilder().setKey("scp").setValue("scpval1").build();

            Map<String, Annotation> jobAnnotations = ImmutableMap.of(
                    fromJob1.getKey(), fromJob1,
                    fromJob2.getKey(), fromJob2,
                    fromJob3.getKey(), fromJob3,
                    fromJob4.getKey(), fromJob4);

            Annotation expected1 = Annotation.newBuilder().setKey("bs1").setValue("bs1val2").build();
            Annotation expected2 = Annotation.newBuilder().setKey("bs2").setValue("bs2val1").build();
            Annotation expected3 = Annotation.newBuilder().setKey("bs3").setValue("bs3val2").build();
            Annotation expected4 = Annotation.newBuilder().setKey("scp").setValue("scpval1").build();

            assertThat(JobExecutionUtil.GetScriptAnnotationOverridesForSeed(seed.build(), jobConfig.build(), jobAnnotations))
                    .hasSize(4)
                    .containsOnlyKeys("bs1", "bs2", "bs3", "scp")
                    .containsValues(expected1, expected2, expected3, expected4);
        }
    }

    @Test
    void overrideAnnotation() {
        Annotation k1 = Annotation.newBuilder().setKey("k1").setValue("v1").build();
        Annotation k2 = Annotation.newBuilder().setKey("k2").setValue("v2").build();
        Annotation k2jc1 = Annotation.newBuilder().setKey("{jc1}k2").setValue("v2jc1").build();
        Annotation k3id1 = Annotation.newBuilder().setKey("{id1}k3").setValue("v3id1").build();
        Annotation k3 = Annotation.newBuilder().setKey("k3").setValue("v3").build();
        Annotation k4 = Annotation.newBuilder().setKey("k4").setValue("v4").build();

        List<Annotation> annotations = new ArrayList<>();
        annotations.add(Annotation.newBuilder().setKey("k1").setValue("v1").build());
        annotations.add(Annotation.newBuilder().setKey("k2").setValue("v2").build());
        annotations.add(Annotation.newBuilder().setKey("{jc1}k2").setValue("v2jc1").build());
        annotations.add(Annotation.newBuilder().setKey("{id1}k3").setValue("v3id1").build());
        annotations.add(Annotation.newBuilder().setKey("k3").setValue("v3").build());
        annotations.add(Annotation.newBuilder().setKey("k4").setValue("v4").build());

        ConfigObject jobConfig = ConfigObject.newBuilder().setId("id1").setMeta(Meta.newBuilder().setName("jc1")).build();

        Map<String, Annotation> jobAnnotations = new HashMap<>();
        jobAnnotations.put("k1", Annotation.newBuilder().setKey("k1").setValue("value").build());
        jobAnnotations.put("k2", Annotation.newBuilder().setKey("k2").setValue("value").build());
        jobAnnotations.put("k3", Annotation.newBuilder().setKey("k3").setValue("value").build());

        JobExecutionUtil.overrideAnnotation(annotations, jobConfig, jobAnnotations);

        Annotation expected1 = Annotation.newBuilder().setKey("k1").setValue("v1").build();
        Annotation expected2 = Annotation.newBuilder().setKey("k2").setValue("v2jc1").build();
        Annotation expected3 = Annotation.newBuilder().setKey("k3").setValue("v3id1").build();

        assertThat(jobAnnotations)
                .containsOnlyKeys("k1", "k2", "k3")
                .containsValues(expected1, expected2, expected3);
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