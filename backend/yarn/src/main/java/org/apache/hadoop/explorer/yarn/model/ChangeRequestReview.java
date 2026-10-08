package org.apache.hadoop.explorer.yarn.model;

import jakarta.validation.constraints.Size;

public class ChangeRequestReview {
    @Size(max = 1000)
    private String comment = "";

    public ChangeRequestReview() {}

    public ChangeRequestReview(String comment) {
        this.comment = comment != null ? comment : "";
    }

    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }
}
