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
package no.nb.nna.veidemann.controller.settings;

import no.nb.nna.veidemann.commons.settings.CommonSettings;

/**
 * Configuration settings for Veidemann Controller.
 */
public class Settings extends CommonSettings {

    private int apiPort;

    private String frontierHost;

    private int frontierPort;

    private String scopeserviceHost;

    private int scopeservicePort;

    private String logServiceHost;

    private int logServicePort;

    private String openIdConnectIssuer;

    private String apiKeyRoleMappingFile;

    private boolean skipAuthentication;

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public String getFrontierHost() {
        return frontierHost;
    }

    public void setFrontierHost(String frontierHost) {
        this.frontierHost = frontierHost;
    }

    public int getFrontierPort() {
        return frontierPort;
    }

    public void setFrontierPort(int frontierPort) {
        this.frontierPort = frontierPort;
    }

    public String getScopeserviceHost() {
        return scopeserviceHost;
    }

    public void setScopeserviceHost(String scopeserviceHost) {
        this.scopeserviceHost = scopeserviceHost;
    }

    public int getScopeservicePort() {
        return scopeservicePort;
    }

    public void setScopeservicePort(int scopeservicePort) {
        this.scopeservicePort = scopeservicePort;
    }

    public String getOpenIdConnectIssuer() {
        return openIdConnectIssuer;
    }

    public void setOpenIdConnectIssuer(String openIdConnectIssuer) {
        this.openIdConnectIssuer = openIdConnectIssuer;
    }

    public String getApiKeyRoleMappingFile() {
        return apiKeyRoleMappingFile;
    }

    public void setApiKeyRoleMappingFile(String apiKeyRoleMappingFile) {
        this.apiKeyRoleMappingFile = apiKeyRoleMappingFile;
    }

    public boolean getSkipAuthentication() {
        return skipAuthentication;
    }

    public void setSkipAuthentication(boolean skipAuthentication) {
        this.skipAuthentication = skipAuthentication;
    }

    public String getLogServiceHost() {
        return logServiceHost;
    }

    public void setLogServiceHost(String logServiceHost) {
        this.logServiceHost = logServiceHost;
    }

    public int getLogServicePort() {
        return logServicePort;
    }

    public void setLogServicePort(int logServicePort) {
        this.logServicePort = logServicePort;
    }
}
