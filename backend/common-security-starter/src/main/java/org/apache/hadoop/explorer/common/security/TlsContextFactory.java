package org.apache.hadoop.explorer.common.security;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;

/**
 * Фабрика для создания SSLContext и настройки HTTP/REST клиентов с поддержкой TLS,
 * кастомных Keystore/Truststore и режима тестирования insecureSkipVerify.
 */
public final class TlsContextFactory {

    private static final Logger log = LoggerFactory.getLogger(TlsContextFactory.class);

    private TlsContextFactory() {}

    /**
     * Создает javax.net.ssl.SSLContext на основе переданных настроек безопасности.
     */
    public static SSLContext createSslContext(CommonSecurityProperties.TlsProperties props) {
        if (props == null || !props.isEnabled()) {
            try {
                return SSLContext.getDefault();
            } catch (Exception e) {
                log.warn("Не удалось получить стандартный SSLContext: {}", e.getMessage());
                return createTrustAllSslContext();
            }
        }

        try {
            if (props.isInsecureSkipVerify()) {
                log.warn("ВНИМАНИЕ: Включен insecureSkipVerify! Проверка TLS-сертификатов отключена.");
                return createTrustAllSslContext();
            }

            KeyManager[] keyManagers = null;
            TrustManager[] trustManagers = null;

            // 1. Настройка KeyManager (для mTLS клиентов или сервера)
            String keyStorePath = props.getKeyStorePath();
            if (keyStorePath == null && props.isAutoGenerateSelfSigned()) {
                File selfSignedKeystore = TlsCertificateGenerator.getOrCreateSelfSignedKeystore(props.getKeyStorePassword());
                keyStorePath = selfSignedKeystore.getAbsolutePath();
            }

            if (keyStorePath != null) {
                KeyStore ks = loadKeyStore(keyStorePath, props.getKeyStorePassword(), props.getKeyStoreType());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                char[] passChars = (props.getKeyStorePassword() != null) ? props.getKeyStorePassword().toCharArray() : new char[0];
                kmf.init(ks, passChars);
                keyManagers = kmf.getKeyManagers();
            }

            // 2. Настройка TrustManager (доверенные CA / TrustStore)
            String trustStorePath = props.getTrustStorePath();
            if (trustStorePath != null) {
                KeyStore ts = loadKeyStore(trustStorePath, props.getTrustStorePassword(), props.getTrustStoreType());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            String protocol = (props.getEnabledProtocols() != null && !props.getEnabledProtocols().isEmpty())
                    ? props.getEnabledProtocols().get(0)
                    : "TLS";

            SSLContext sslContext = SSLContext.getInstance(protocol);
            sslContext.init(keyManagers, trustManagers, new SecureRandom());
            return sslContext;

        } catch (Exception e) {
            log.error("Ошибка при инициализации TLS SSLContext: {}", e.getMessage(), e);
            throw new IllegalStateException("Не удалось настроить TLS SSLContext: " + e.getMessage(), e);
        }
    }

    /**
     * Создает SSLContext, доверяющий любым сертификатам (для тестов и dev-сред).
     */
    public static SSLContext createTrustAllSslContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{ createTrustAllTrustManager() }, new SecureRandom());
            return sslContext;
        } catch (Exception e) {
            throw new IllegalStateException("Не удалось создать Trust-All SSLContext: " + e.getMessage(), e);
        }
    }

    /**
     * Возвращает TrustManager, не проверяющий валидность сертификатов.
     */
    public static X509TrustManager createTrustAllTrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }

    /**
     * Настраивает HttpClient.Builder параметрами SSLContext и протоколов TLS.
     */
    public static HttpClient.Builder configureHttpClient(HttpClient.Builder builder, CommonSecurityProperties.TlsProperties props) {
        if (props != null && props.isEnabled()) {
            SSLContext sslContext = createSslContext(props);
            builder.sslContext(sslContext);

            if (props.getEnabledProtocols() != null && !props.getEnabledProtocols().isEmpty()) {
                SSLParameters params = new SSLParameters();
                params.setProtocols(props.getEnabledProtocols().toArray(new String[0]));
                if (props.getCiphers() != null && !props.getCiphers().isEmpty()) {
                    params.setCipherSuites(props.getCiphers().toArray(new String[0]));
                }
                builder.sslParameters(params);
            }
        }
        return builder;
    }

    /**
     * Создает готовый java.net.http.HttpClient с поддержкой TLS.
     */
    public static HttpClient createHttpClient(CommonSecurityProperties.TlsProperties props, Duration timeout) {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(timeout != null ? timeout : Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL);
        return configureHttpClient(builder, props).build();
    }

    private static KeyStore loadKeyStore(String path, String password, String type) throws Exception {
        String storeType = (type != null && !type.isBlank()) ? type : KeyStore.getDefaultType();
        KeyStore keyStore = KeyStore.getInstance(storeType);
        char[] passChars = (password != null) ? password.toCharArray() : null;

        File file = new File(path);
        if (file.exists()) {
            try (InputStream is = new FileInputStream(file)) {
                keyStore.load(is, passChars);
            }
        } else {
            // Пробуем загрузить из Classpath
            try (InputStream is = TlsContextFactory.class.getResourceAsStream(path.startsWith("/") ? path : "/" + path)) {
                if (is != null) {
                    keyStore.load(is, passChars);
                } else {
                    throw new IllegalArgumentException("Файл KeyStore не найден: " + path);
                }
            }
        }
        return keyStore;
    }
}
