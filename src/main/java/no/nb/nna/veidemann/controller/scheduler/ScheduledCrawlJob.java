/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.controller.scheduler;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.controller.JobExecutionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.calculateTimeout;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCrawlJob.class);

    final ConfigObject job;

    public ScheduledCrawlJob(ConfigObject job) {
        this.job = job;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        LOG.debug("Job '{}' starting", job.getMeta().getName());

        JobExecutionUtil.submitSeeds(job, null, calculateTimeout(job), false, Collections.EMPTY_LIST);
    }
}
