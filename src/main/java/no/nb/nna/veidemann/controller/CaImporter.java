/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class CaImporter {
    private static final Logger LOG = LoggerFactory.getLogger(CaImporter.class);

    public static void importFromFile(String certfile) throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
        String keystorePath = System.getProperty("javax.net.ssl.trustStore", "/etc/ssl/certs/java/cacerts");
        String keystorePasswd = System.getProperty("javax.net.ssl.trustStorePassword", "changeit");

        LOG.info("Loading certificates from {} into trust store at {}", certfile, keystorePath);

        char[] password = keystorePasswd.toCharArray();
        FileInputStream is = new FileInputStream(keystorePath);

        KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        keystore.load(is, password);
        is.close();

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        try (InputStream certstream = new BufferedInputStream(new FileInputStream(certfile));) {
            for (Certificate cert : cf.generateCertificates(certstream)) {
                X509Certificate x509Cert = (X509Certificate) cert;
                String alias = x509Cert.getSubjectDN().getName();
                LOG.info("Adding certificate {} to trust store", alias);
                keystore.setCertificateEntry(alias, cert);
            }
        }

        // Save the new keystore contents
        try (FileOutputStream out = new FileOutputStream(keystorePath);) {
            keystore.store(out, password);
        }
    }
}