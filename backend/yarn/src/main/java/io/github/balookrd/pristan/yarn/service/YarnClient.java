package io.github.balookrd.pristan.yarn.service;

import io.github.balookrd.pristan.yarn.model.ClusterMetrics;
import io.github.balookrd.pristan.yarn.model.QueueNode;

public interface YarnClient {

    /**
     * Возвращает метрики кластера YARN.
     */
    ClusterMetrics getClusterMetrics(String doAs);

    /**
     * Возвращает корневой узел дерева очередей YARN.
     */
    QueueNode getQueueTree(String doAs);

    /**
     * Возвращает URL активного ResourceManager.
     */
    String getActiveRmUrl();
}
