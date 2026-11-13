package io.github.balookrd.pristan.spark.dto;

public record NodePosition(double x, double y) {
    public NodePosition() {
        this(0.0, 0.0);
    }
}
