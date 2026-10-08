package org.apache.hadoop.explorer.common.security;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;

import java.io.File;

/**
 * Кастомайзер сервлетного веб-сервера (Tomcat) для применения параметров TLS из
 * `hadoop.security.tls.*`.
 */
public class TlsWebServerCustomizer implements WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> {

    private static final Logger log = LoggerFactory.getLogger(TlsWebServerCustomizer.class);

    private final CommonSecurityProperties properties;

    public TlsWebServerCustomizer(CommonSecurityProperties properties) {
        this.properties = properties;
    }

    @Override
    public void customize(ConfigurableServletWebServerFactory factory) {
        var tls = properties.getTls();
        if (tls == null || !tls.isEnabled()) {
            return;
        }

        if (factory instanceof org.springframework.boot.web.server.AbstractConfigurableWebServerFactory abstractFactory) {
            if (abstractFactory.getSsl() != null && abstractFactory.getSsl().isEnabled()) {
                log.info("На веб-сервере уже сконфигурирован server.ssl.*, пропускаем кастомизацию hadoop.security.tls");
                return;
            }
        }

        try {
            Ssl ssl = new Ssl();
            ssl.setEnabled(true);

            String keyStorePath = tls.getKeyStorePath();
            if ((keyStorePath == null || keyStorePath.isBlank()) && tls.isAutoGenerateSelfSigned()) {
                File selfSignedKeystore = TlsCertificateGenerator.getOrCreateSelfSignedKeystore(tls.getKeyStorePassword());
                keyStorePath = selfSignedKeystore.getAbsolutePath();
            }

            if (keyStorePath != null && !keyStorePath.isBlank()) {
                ssl.setKeyStore(keyStorePath);
                ssl.setKeyStorePassword(tls.getKeyStorePassword());
                ssl.setKeyStoreType(tls.getKeyStoreType());
            }

            if (tls.getKeyAlias() != null && !tls.getKeyAlias().isBlank()) {
                ssl.setKeyAlias(tls.getKeyAlias());
            }

            if (tls.getTrustStorePath() != null && !tls.getTrustStorePath().isBlank()) {
                ssl.setTrustStore(tls.getTrustStorePath());
                ssl.setTrustStorePassword(tls.getTrustStorePassword());
                ssl.setTrustStoreType(tls.getTrustStoreType());
            }

            if (tls.getClientAuth() != null) {
                switch (tls.getClientAuth().toLowerCase()) {
                    case "need", "require", "required" -> ssl.setClientAuth(Ssl.ClientAuth.NEED);
                    case "want", "optional" -> ssl.setClientAuth(Ssl.ClientAuth.WANT);
                    default -> ssl.setClientAuth(Ssl.ClientAuth.NONE);
                }
            }

            if (tls.getEnabledProtocols() != null && !tls.getEnabledProtocols().isEmpty()) {
                ssl.setEnabledProtocols(tls.getEnabledProtocols().toArray(new String[0]));
            }

            if (tls.getCiphers() != null && !tls.getCiphers().isEmpty()) {
                ssl.setCiphers(tls.getCiphers().toArray(new String[0]));
            }

            factory.setSsl(ssl);
            log.info("Включен HTTPS/TLS для REST API веб-сервера (keystore: {}, clientAuth: {})",
                    keyStorePath, ssl.getClientAuth());

        } catch (Exception e) {
            log.error("Ошибка при настройке TLS на сервлетном сервере: {}", e.getMessage(), e);
            throw new IllegalStateException("Не удалось применить TLS к веб-серверу: " + e.getMessage(), e);
        }
    }
}
