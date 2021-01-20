package no.nb.nna.veidemann.controller;

import no.nb.nna.veidemann.controller.ControllerApiServer.JobExecutionListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JobExecutionStartedListener implements JobExecutionListener {
    private CountDownLatch started = new CountDownLatch(1);

    @Override
    public void onJobStarting(String jobExecutionId) {

    }

    @Override
    public void onJobStarted(String jobExecutionId) {
        started.countDown();
    }

    public boolean waitForStarted(long timeout, TimeUnit unit) {
        try {
            return started.await(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
