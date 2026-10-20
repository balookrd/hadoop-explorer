package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.explorer.replicator.generated.FileMetadata;
import org.apache.hadoop.explorer.replicator.generated.TarStreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Валидатор криптографической аттестации задач репликации (Zero-Trust Task Attestation).
 * Выполняет проверку криптографической подписи HMAC и контекста безопасности (SEC-01)
 * перед приемом файлов и виртуальных TAR-стримов на Receiver-сервере.
 */
public class TaskAttestationValidator {

    private static final Logger logger = LoggerFactory.getLogger(TaskAttestationValidator.class);

    private final String agentSecret;
    private final boolean enforceTaskAttestation;

    public TaskAttestationValidator(String agentSecret, boolean enforceTaskAttestation) {
        this.agentSecret = agentSecret;
        this.enforceTaskAttestation = enforceTaskAttestation;
    }

    public boolean isEnforceTaskAttestation() {
        return enforceTaskAttestation;
    }

    public void validateTaskAttestation(FileMetadata metadata) {
        if (metadata == null) return;
        validateTaskAttestationInternal(
                metadata.getTaskToken(),
                metadata.getTaskId(),
                metadata.getJobId(),
                metadata.getTargetPath(),
                metadata.getExecutionPrincipal(),
                metadata.getRunAsServiceAccount()
        );
    }

    public void validateTaskAttestation(TarStreamMetadata metadata) {
        if (metadata == null) return;
        validateTaskAttestationInternal(
                metadata.getTaskToken(),
                metadata.getTaskId(),
                metadata.getJobId(),
                metadata.getBaseTargetPath(),
                metadata.getExecutionPrincipal(),
                metadata.getRunAsServiceAccount()
        );
    }

    public void validateTaskAttestationInternal(String token, String taskId, String jobId,
                                                 String targetPath, String executionPrincipal,
                                                 boolean runAsServiceAccount) {
        boolean hasToken = token != null && !token.isBlank();
        if (hasToken) {
            if (agentSecret == null || agentSecret.isBlank()) {
                logger.warn("Получен токен аттестации задачи, но секретный ключ agentSecret не сконфигурирован на Receiver сервере");
                if (enforceTaskAttestation) {
                    throw new SecurityException("Аттестация задачи отклонена: секретный ключ ресивера не сконфигурирован");
                }
                return;
            }
            boolean valid = TaskAttestationService.validateToken(
                    token, taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount, agentSecret
            );
            if (!valid) {
                logger.error("Отказ безопасности (SEC-01): недействительный токен аттестации задачи task={}, job={}, path={}, principal={}, runAsService={}",
                        taskId, jobId, targetPath, executionPrincipal, runAsServiceAccount);
                throw new SecurityException("Криптографическая аттестация задачи не пройдена: токен подделан или параметры скомпрометированы");
            }
            logger.debug("Криптографическая аттестация задачи подтверждена: task={}, job={}, principal={}",
                    taskId, jobId, executionPrincipal);
        } else {
            if (enforceTaskAttestation) {
                logger.error("Отказ безопасности (SEC-01): отсутствует обязательный токен аттестации задачи task={}, job={}",
                        taskId, jobId);
                throw new SecurityException("Отклонено: требуется криптографическая аттестация задачи (Zero-Trust Task Attestation Token)");
            }
            logger.debug("Токен аттестации задачи отсутствует, выполнение в доверенном режиме обратной совместимости (task={}, job={})",
                    taskId, jobId);
        }
    }
}
