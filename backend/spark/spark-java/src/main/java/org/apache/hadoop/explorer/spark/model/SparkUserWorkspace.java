package org.apache.hadoop.explorer.spark.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "spark_user_workspaces")
public class SparkUserWorkspace {

    @Id
    @Column(name = "username", length = 128, nullable = false)
    private String username;

    @Lob
    @Column(name = "state_json", nullable = false)
    private String stateJson;

    @Column(name = "updated_at")
    private Instant updatedAt = Instant.now();

    public SparkUserWorkspace() {
    }

    public SparkUserWorkspace(String username, String stateJson, Instant updatedAt) {
        this.username = username;
        this.stateJson = stateJson;
        this.updatedAt = updatedAt;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getStateJson() {
        return stateJson;
    }

    public void setStateJson(String stateJson) {
        this.stateJson = stateJson;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
