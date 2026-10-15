package org.apache.hadoop.explorer.replicator.kafka.security;

import java.net.URI;

/**
 * Интерфейс генератора HTTP-заголовков Kerberos SPNEGO (RFC 4559 Negotiate)
 * для защищенного доступа к REST API Schema Registry (Confluent и Hortonworks).
 */
public interface SpnegoAuthenticator {

    /**
     * Генерирует HTTP заголовок авторизации Negotiate для целевого URI.
     *
     * @param targetUri URI реестра схем (например: http://sr1.corp:8081)
     * @return строка вида "Negotiate YIIEvAYJKoZIhvcSAQICAQB...", либо null если аутентификация отключена
     */
    String generateNegotiateHeader(URI targetUri);
}
