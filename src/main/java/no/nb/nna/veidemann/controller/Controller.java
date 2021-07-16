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
package no.nb.nna.veidemann.controller;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import no.nb.nna.veidemann.commons.auth.UserRoleMapper;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.controller.scheduler.CrawlJobScheduler;
import no.nb.nna.veidemann.controller.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class Controller {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);

        Tracer tracer = Configuration.fromEnv().getTracer();
        GlobalTracer.registerIfAbsent(tracer);
    }

    public Controller() {
        File certFile = new File("/veidemann/trustedca/tls.crt");
        if (certFile.exists()) {
            try {
                CaImporter.importFromFile("/veidemann/trustedca/tls.crt");
            } catch (Exception e) {
                LOG.error("Failed loading certificates", e);
            }
        }
    }

    /**
     * Start the controller service.
     * <p>
     *
     * @return this instance
     */
    public Controller start() {
        UserRoleMapper userRoleMapper = new UserRoleMapper();
        try (DbService db = DbService.configure(SETTINGS);
             FrontierClient urlFrontierClient = new FrontierClient(SETTINGS.getFrontierHost(), SETTINGS
                     .getFrontierPort(), "url");
             ScopeServiceClient scopeServiceClient = new ScopeServiceClient(SETTINGS.getScopeserviceHost(), SETTINGS
                     .getScopeservicePort());
             LogServiceClient logServiceClient = new LogServiceClient(SETTINGS.getLogServiceHost(), SETTINGS.getLogServicePort());
             ControllerApiServer apiServer = new ControllerApiServer(SETTINGS, userRoleMapper, scopeServiceClient, logServiceClient);
             CrawlJobScheduler scheduler = new CrawlJobScheduler()) {

            registerShutdownHook();
            scheduler.start();
            apiServer.start();
            LOG.info("Veidemann Controller (v. {}) started", Controller.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException | DbException ex) {
            LOG.error("Configuration error: {}", ex.getLocalizedMessage());
            System.exit(1);
        }

        return this;
    }

    private void registerShutdownHook() {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down since JVM is shutting down");

            mainThread.interrupt();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                // pass
            }
            System.err.println("*** gracefully shut down");
        }));
    }

    /**
     * Get the settings object.
     * <p>
     *
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
