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
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.controller.JobExecutionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static no.nb.nna.veidemann.controller.JobExecutionUtil.calculateTimeout;

/**
 *
 */
public class ScheduledCrawlJob extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledCrawlJob.class);

    final ConfigObject job;

    static final Lock lock = new ReentrantLock();

    public ScheduledCrawlJob(ConfigObject job) {
        this.job = job;
    }

    @Override
    public void execute(TaskExecutionContext context) throws RuntimeException {
        lock.lock();
        try {
            JobExecutionStatus jobExecutionStatus = JobExecutionUtil.getRunningJobExecutionStatusForJob(job);
            if (jobExecutionStatus == null) {
                LOG.debug("Job '{}' starting", job.getMeta().getName());
                JobExecutionUtil.submitSeeds(job, null, calculateTimeout(job), false, Collections.emptyList());
            } else {
                LOG.info("The job '{}' is already running. Job execution: '{}'", job.getMeta().getName(), jobExecutionStatus.getId());
            }
        } catch (DbException e) {
            LOG.warn("Could not start scheduled job '{}'.", job.getMeta().getName());
        } finally {
            lock.unlock();
        }
    }
}
