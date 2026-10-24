package org.apache.hadoop.explorer.replicator.orchestrator.security;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.registry.AgentRegistry;
import org.apache.hadoop.explorer.replicator.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Безопасные значения по умолчанию.
 *
 * <p>Все защитные механизмы платформы были реализованы, но выключены в дефолтной конфигурации:
 * секрет агента зашит в код, проверка секрета и белого списка кластеров отключены. Установка,
 * которую не донастроили, оказывалась полностью открытой, а в README при этом числился
 * список устраненных уязвимостей.
 */
@DisplayName("Безопасные дефолты оркестратора")
class SecureDefaultsTest {

    @Test
    @DisplayName("Проверка секрета агента включена по умолчанию")
    void testAgentSecretEnforcementIsOnByDefault() {
        assertTrue(new ReplicatorProperties().isEnforceAgentSecret(),
                "enforce-agent-secret по умолчанию должен быть включен");
    }

    @Test
    @DisplayName("Белый список кластеров включен по умолчанию")
    void testClusterWhitelistIsOnByDefault() {
        assertTrue(new ReplicatorProperties().isEnforceClusterWhitelist(),
                "enforce-cluster-whitelist по умолчанию должен быть включен");
    }

    @Test
    @DisplayName("Секрет агента не зашит в код: по умолчанию он не задан")
    void testAgentSecretIsNotHardcoded() {
        String secret = new ReplicatorProperties().getAgentSecret();
        assertTrue(secret == null || secret.isBlank(),
                "Дефолтный секрет в коде означает единый публично известный ключ для всех установок, получено: " + secret);
    }

    @Test
    @DisplayName("Старт с незаданным секретом в защищенном режиме прерывается")
    void testStartupFailsWhenSecretMissingInEnforcedMode() {
        ReplicatorProperties properties = new ReplicatorProperties();
        properties.setEnforceAgentSecret(true);
        properties.setAgentSecret(null);
        AgentRegistry registry = new AgentRegistry(properties, new TopologyRegistry(properties));

        IllegalStateException ex = assertThrows(IllegalStateException.class, registry::initSecurityValidation);
        assertTrue(ex.getMessage().contains("agent-secret"),
                "Сообщение должно указывать на незаданный секрет, получено: " + ex.getMessage());
    }

    @Test
    @DisplayName("Старт с коротким секретом в защищенном режиме прерывается")
    void testStartupFailsWhenSecretTooShort() {
        ReplicatorProperties properties = new ReplicatorProperties();
        properties.setEnforceAgentSecret(true);
        properties.setAgentSecret("short");
        AgentRegistry registry = new AgentRegistry(properties, new TopologyRegistry(properties));

        assertThrows(IllegalStateException.class, registry::initSecurityValidation);
    }

    @Test
    @DisplayName("Старт с корректным секретом проходит")
    void testStartupPassesWithStrongSecret() {
        ReplicatorProperties properties = new ReplicatorProperties();
        properties.setEnforceAgentSecret(true);
        properties.setAgentSecret("a-sufficiently-long-unique-secret");
        AgentRegistry registry = new AgentRegistry(properties, new TopologyRegistry(properties));

        assertDoesNotThrow(registry::initSecurityValidation);
    }
}
