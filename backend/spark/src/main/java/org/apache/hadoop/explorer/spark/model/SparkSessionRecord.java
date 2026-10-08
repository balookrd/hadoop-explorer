package org.apache.hadoop.explorer.spark.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "spark_sessions")
public class SparkSessionRecord {

    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    @Column(name = "username", length = 128, nullable = false)
    private String username;

    @Column(name = "cluster_id", length = 64, nullable = false)
    private String clusterId;

    @Column(name = "spark_version_id", length = 64, nullable = false)
    private String sparkVersionId;

    @Column(name = "python_env_id", length = 64)
    private String pythonEnvId;

    @Column(name = "custom_python_archive", length = 512)
    private String customPythonArchive;

    @Column(name = "custom_python_path", length = 256)
    private String customPythonPath;

    @Column(name = "metastore_id", length = 64, nullable = false)
    private String metastoreId;

    @Column(name = "yarn_queue", length = 128, nullable = false)
    private String yarnQueue;

    @Column(name = "resource_profile", length = 64, nullable = false)
    private String resourceProfile;

    @Column(name = "kind", length = 32, nullable = false)
    private String kind = "pyspark"; // pyspark, spark (Scala), sql

    @Column(name = "livy_session_id")
    private Integer livySessionId;

    @Column(name = "yarn_application_id", length = 128)
    private String yarnApplicationId;

    @Column(name = "status", length = 32, nullable = false)
    private String status = "idle"; // not_started, starting, idle, busy, dead, killed

    @Lob
    @Column(name = "packages_json")
    private String packagesJson;

    @Lob
    @Column(name = "jars_json")
    private String jarsJson;

    @Lob
    @Column(name = "py_files_json")
    private String pyFilesJson;

    @Lob
    @Column(name = "spark_conf_json")
    private String sparkConfJson;

    @Column(name = "created_at")
    private Instant createdAt = Instant.now();

    @Column(name = "last_activity_at")
    private Instant lastActivityAt = Instant.now();

    @Column(name = "stopped_at")
    private Instant stoppedAt;

    public SparkSessionRecord() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getSparkVersionId() {
        return sparkVersionId;
    }

    public void setSparkVersionId(String sparkVersionId) {
        this.sparkVersionId = sparkVersionId;
    }

    public String getPythonEnvId() {
        return pythonEnvId;
    }

    public void setPythonEnvId(String pythonEnvId) {
        this.pythonEnvId = pythonEnvId;
    }

    public String getCustomPythonArchive() {
        return customPythonArchive;
    }

    public void setCustomPythonArchive(String customPythonArchive) {
        this.customPythonArchive = customPythonArchive;
    }

    public String getCustomPythonPath() {
        return customPythonPath;
    }

    public void setCustomPythonPath(String customPythonPath) {
        this.customPythonPath = customPythonPath;
    }

    public String getMetastoreId() {
        return metastoreId;
    }

    public void setMetastoreId(String metastoreId) {
        this.metastoreId = metastoreId;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public String getResourceProfile() {
        return resourceProfile;
    }

    public void setResourceProfile(String resourceProfile) {
        this.resourceProfile = resourceProfile;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public Integer getLivySessionId() {
        return livySessionId;
    }

    public void setLivySessionId(Integer livySessionId) {
        this.livySessionId = livySessionId;
    }

    public String getYarnApplicationId() {
        return yarnApplicationId;
    }

    public void setYarnApplicationId(String yarnApplicationId) {
        this.yarnApplicationId = yarnApplicationId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPackagesJson() {
        return packagesJson;
    }

    public void setPackagesJson(String packagesJson) {
        this.packagesJson = packagesJson;
    }

    public String getJarsJson() {
        return jarsJson;
    }

    public void setJarsJson(String jarsJson) {
        this.jarsJson = jarsJson;
    }

    public String getPyFilesJson() {
        return pyFilesJson;
    }

    public void setPyFilesJson(String pyFilesJson) {
        this.pyFilesJson = pyFilesJson;
    }

    public String getSparkConfJson() {
        return sparkConfJson;
    }

    public void setSparkConfJson(String sparkConfJson) {
        this.sparkConfJson = sparkConfJson;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getLastActivityAt() {
        return lastActivityAt;
    }

    public void setLastActivityAt(Instant lastActivityAt) {
        this.lastActivityAt = lastActivityAt;
    }

    public Instant getStoppedAt() {
        return stoppedAt;
    }

    public void setStoppedAt(Instant stoppedAt) {
        this.stoppedAt = stoppedAt;
    }
}
