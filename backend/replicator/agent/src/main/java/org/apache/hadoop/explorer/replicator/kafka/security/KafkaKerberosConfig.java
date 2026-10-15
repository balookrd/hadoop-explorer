package org.apache.hadoop.explorer.replicator.kafka.security;

import java.io.Serializable;

/**
 * Конфигурация аутентификации Kerberos (GSSAPI / SASL) для клиентов Apache Kafka.
 */
public class KafkaKerberosConfig implements Serializable {

    private boolean enabled = false;
    private String securityProtocol = "SASL_PLAINTEXT"; // SASL_PLAINTEXT или SASL_SSL
    private String serviceName = "kafka";               // Kerberos Service Name (обычно 'kafka')
    private String principal;                           // Например: 'kafka-replicator/host@REALM.COM'
    private String keytabPath;                          // Абсолютный путь к файлу keytab
    private String krb5ConfPath;                        // Опциональный путь к krb5.conf
    private String truststoreLocation;                  // Путь к JKS/PKCS12 truststore (для SASL_SSL)
    private String truststorePassword;
    private boolean srKerberosEnabled = false;          // Включение Kerberos SPNEGO для Schema Registry
    private String srServiceName = "HTTP";              // SPN префикс для Schema Registry (по умолчанию 'HTTP')

    public KafkaKerberosConfig() {
    }

    public KafkaKerberosConfig(boolean enabled, String principal, String keytabPath) {
        this.enabled = enabled;
        this.principal = principal;
        this.keytabPath = keytabPath;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getSecurityProtocol() {
        return securityProtocol != null && !securityProtocol.isBlank() ? securityProtocol : "SASL_PLAINTEXT";
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getServiceName() {
        return serviceName != null && !serviceName.isBlank() ? serviceName : "kafka";
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
    }

    public String getKrb5ConfPath() {
        return krb5ConfPath;
    }

    public void setKrb5ConfPath(String krb5ConfPath) {
        this.krb5ConfPath = krb5ConfPath;
    }

    public String getTruststoreLocation() {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation) {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public boolean isSrKerberosEnabled() {
        return srKerberosEnabled;
    }

    public void setSrKerberosEnabled(boolean srKerberosEnabled) {
        this.srKerberosEnabled = srKerberosEnabled;
    }

    public String getSrServiceName() {
        return srServiceName != null && !srServiceName.isBlank() ? srServiceName : "HTTP";
    }

    public void setSrServiceName(String srServiceName) {
        this.srServiceName = srServiceName;
    }

    @Override
    public String toString() {
        return "KafkaKerberosConfig{" +
                "enabled=" + enabled +
                ", securityProtocol='" + securityProtocol + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", principal='" + principal + '\'' +
                ", keytabPath='" + keytabPath + '\'' +
                ", srKerberosEnabled=" + srKerberosEnabled +
                ", srServiceName='" + srServiceName + '\'' +
                '}';
    }
}
