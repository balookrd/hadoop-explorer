package org.apache.hadoop.explorer.replicator.orchestrator.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.persistence.*;

import java.time.Instant;

/**
 * Сущность подзадачи репликации (единичный файл или Tar-бандл мелких файлов).
 *
 * <p><b>Архитектурное решение по партиционированию и первичному ключу:</b>
 * В СУБД PostgreSQL таблица {@code replication_tasks} партиционирована по диапазонам дат:
 * {@code PARTITION BY RANGE (updated_at)}.
 * По стандарту PostgreSQL декларативные первичные ключи на секционированных таблицах
 * обязаны включать столбец партиционирования (т.е. {@code PRIMARY KEY (id, updated_at)}).
 * В JPA/Hibernate поле {@code id} является единственным идентификатором ({@code @Id}),
 * поскольку {@code updated_at} непрерывно мутирует при переходах статусов задачи
 * (QUEUED -> RUNNING -> COMPLETED/FAILED), а мутация первичного ключа в JPA запрещена.
 * Поиск по {@code id} в секциях обслуживается ведущей колонкой составного индекса B-Tree (Index Scan).
 */
@Entity
@Table(
    name = "replication_tasks",
    indexes = {
        @Index(name = "idx_task_job_id", columnList = "job_id"),
        @Index(name = "idx_task_status", columnList = "status"),
        @Index(name = "idx_task_job_status", columnList = "job_id, status"),
        @Index(name = "idx_task_status_job_id", columnList = "status, job_id"),
        @Index(name = "idx_task_status_updated_at", columnList = "status, updated_at")
    }
)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TaskEntity implements org.springframework.data.domain.Persistable<String> {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    /**
     * Признак еще не сохраненной задачи.
     *
     * <p>Идентификатор задачи присваивается кодом, поэтому без {@code Persistable} Spring Data
     * считает сущность «возможно существующей» и сохраняет ее через {@code em.merge()}:
     * перед каждой вставкой выполняется SELECT. На пакете анализа из десятков тысяч файлов
     * это такое же число одиночных запросов, и JDBC-батчинг ускоряет только сам INSERT.
     * Флаг транзиентный: после сохранения или загрузки сущность перестает быть новой.
     */
    @Transient
    @JsonIgnore
    private boolean persisted = false;

    @Column(name = "job_id", nullable = false, length = 64)
    private String jobId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", insertable = false, updatable = false)
    @JsonIgnore
    private JobEntity job;

    @Column(name = "run_id", length = 64)
    private String runId;

    @Column(name = "source_path", nullable = false, length = ColumnText.PATH_LENGTH)
    private String sourcePath;

    @Column(name = "target_path", nullable = false, length = ColumnText.PATH_LENGTH)
    private String targetPath;

    @Column(name = "file_size", nullable = false)
    private long fileSize = 0;

    @Column(name = "status", nullable = false, length = 32)
    private String status = "QUEUED";

    @Column(name = "assigned_agent_id", length = 64)
    private String assignedAgentId;

    @Column(name = "checksum")
    private String checksum;

    @Column(name = "error_message", length = ColumnText.MESSAGE_LENGTH)
    private String errorMessage;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries = 3;

    @Column(name = "last_failed_agent_id", length = 64)
    private String lastFailedAgentId;

    @Column(name = "task_type", nullable = false, length = 32)
    private String taskType = "FILE";

    @Column(name = "file_count", nullable = false)
    private int fileCount = 1;

    @Column(name = "bundle_manifest", columnDefinition = "TEXT")
    private String bundleManifest;

    @Column(name = "updated_at")
    private Instant updatedAt = Instant.now();

    public TaskEntity() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public JobEntity getJob() { return job; }
    public void setJob(JobEntity job) { this.job = job; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getSourcePath() { return sourcePath; }
    public void setSourcePath(String sourcePath) { this.sourcePath = sourcePath; }
    public String getTargetPath() { return targetPath; }
    public void setTargetPath(String targetPath) { this.targetPath = targetPath; }
    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getAssignedAgentId() { return assignedAgentId; }
    public void setAssignedAgentId(String assignedAgentId) { this.assignedAgentId = assignedAgentId; }
    public String getChecksum() { return checksum; }
    public void setChecksum(String checksum) { this.checksum = checksum; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = ColumnText.message(errorMessage); }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public String getLastFailedAgentId() { return lastFailedAgentId; }
    public void setLastFailedAgentId(String lastFailedAgentId) { this.lastFailedAgentId = lastFailedAgentId; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }

    public int getFileCount() { return fileCount; }
    public void setFileCount(int fileCount) { this.fileCount = fileCount; }

    public String getBundleManifest() { return bundleManifest; }
    public void setBundleManifest(String bundleManifest) { this.bundleManifest = bundleManifest; }

    @Override
    @JsonIgnore
    public boolean isNew() {
        return !persisted;
    }

    @jakarta.persistence.PostPersist
    @jakarta.persistence.PostLoad
    void markPersisted() {
        this.persisted = true;
    }
}
