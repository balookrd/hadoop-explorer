package org.apache.hadoop.explorer.yarn.service;

import org.apache.hadoop.explorer.yarn.model.ClusterMetrics;
import org.apache.hadoop.explorer.yarn.model.QueueNode;

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
     * Возвращает текущее содержимое capacity-scheduler.xml из YARN RM.
     */
    String getCapacitySchedulerXml(String doAs);

    /**
     * Возвращает URL активного ResourceManager.
     */
    String getActiveRmUrl();
}
