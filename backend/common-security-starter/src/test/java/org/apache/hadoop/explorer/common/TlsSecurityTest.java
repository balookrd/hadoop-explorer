package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.security.TlsCertificateGenerator;
import org.apache.hadoop.explorer.common.security.TlsContextFactory;
import org.apache.hadoop.explorer.common.security.TlsWebServerCustomizer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Тестирование TLS/SSL конфигурации и утилит платформы")
class TlsSecurityTest {

    @Test
    @DisplayName("Генерация временного самоподписанного PKCS12 Keystore")
    void testSelfSignedKeystoreGeneration() throws Exception {
        File keystore = TlsCertificateGenerator.getOrCreateSelfSignedKeystore("testpass123");
        assertNotNull(keystore);
        assertTrue(keystore.exists());
        assertTrue(keystore.length() > 0);
    }

    @Test
    @DisplayName("Создание SSLContext с insecureSkipVerify=true")
    void testInsecureSkipVerifySslContext() {
        CommonSecurityProperties.TlsProperties props = new CommonSecurityProperties.TlsProperties();
        props.setEnabled(true);
        props.setInsecureSkipVerify(true);

        SSLContext sslContext = TlsContextFactory.createSslContext(props);
        assertNotNull(sslContext);
        assertEquals("TLS", sslContext.getProtocol());
    }

    @Test
    @DisplayName("Создание SSLContext с самоподписанным Keystore")
    void testSelfSignedSslContext() {
        CommonSecurityProperties.TlsProperties props = new CommonSecurityProperties.TlsProperties();
        props.setEnabled(true);
        props.setAutoGenerateSelfSigned(true);
        props.setKeyStorePassword("testpass123");

        SSLContext sslContext = TlsContextFactory.createSslContext(props);
        assertNotNull(sslContext);
    }

    @Test
    @DisplayName("Создание HttpClient с поддержкой TLS")
    void testCreateHttpClientWithTls() {
        CommonSecurityProperties.TlsProperties props = new CommonSecurityProperties.TlsProperties();
        props.setEnabled(true);
        props.setInsecureSkipVerify(true);
        props.setEnabledProtocols(List.of("TLSv1.3", "TLSv1.2"));

        HttpClient client = TlsContextFactory.createHttpClient(props, Duration.ofSeconds(5));
        assertNotNull(client);
        assertNotNull(client.sslContext());
    }

    @Test
    @DisplayName("Кастомизация сервлетного веб-сервера через TlsWebServerCustomizer")
    void testTlsWebServerCustomizer() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        props.getTls().setEnabled(true);
        props.getTls().setClientAuth("need");
        props.getTls().setAutoGenerateSelfSigned(true);

        org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory factory =
                new org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory();

        TlsWebServerCustomizer customizer = new TlsWebServerCustomizer(props);
        customizer.customize(factory);

        assertNotNull(factory.getSsl());
        assertTrue(factory.getSsl().isEnabled());
        assertNotNull(factory.getSsl().getKeyStore());
        assertEquals(org.springframework.boot.web.server.Ssl.ClientAuth.NEED, factory.getSsl().getClientAuth());
    }
}
