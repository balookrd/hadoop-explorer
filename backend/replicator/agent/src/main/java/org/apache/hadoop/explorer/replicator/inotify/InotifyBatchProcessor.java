package org.apache.hadoop.explorer.replicator.inotify;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.explorer.replicator.client.OrchestratorClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.model.BatchCreateTasksRequest;
import org.apache.hadoop.explorer.replicator.model.JobDto;
import org.apache.hadoop.explorer.replicator.model.TaskCreateItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Процессор событий HDFS Inotify.
 * Распознает коммиты Spark/Hive (как пофайловые, так и перемещение партиций целиком),
 * игнорирует промежуточные staging пути и формирует задачи репликации в оркестраторе.
 */
public class InotifyBatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(InotifyBatchProcessor.class);

    private final HadoopFsManager fsManager;
    private final OrchestratorClient orchestratorClient;

    public InotifyBatchProcessor(HadoopFsManager fsManager, OrchestratorClient orchestratorClient) {
        this.fsManager = fsManager;
        this.orchestratorClient = orchestratorClient;
    }

    /**
     * Обработка одного HDFS Inotify события в контексте активных streaming-задач.
     * Возвращает количество созданных задач репликации.
     */
    public int processEvent(Event event, long txid, long lag, List<JobDto> activeStreamingJobs) {
        if (event == null || activeStreamingJobs == null || activeStreamingJobs.isEmpty()) {
            return 0;
        }

        int totalCreated = 0;

        for (JobDto job : activeStreamingJobs) {
            String srcPrefix = job.getSourcePath();

            if (event instanceof Event.CloseEvent closeEvent) {
                String path = closeEvent.getPath();
                if (InotifyPathFilter.matchesJobPrefix(path, srcPrefix) && !InotifyPathFilter.isStagedPath(path)) {
                    log.debug("[Inotify CloseEvent] Зафиксирован файл {} (размер {} байт, txid {})", path, closeEvent.getFileSize(), txid);
                    int created = enqueueFile(job, path, closeEvent.getFileSize());
                    totalCreated += created;
                    orchestratorClient.updateJobStreamingTxid(job.getId(), txid, lag);
                }
            } else if (event instanceof Event.RenameEvent renameEvent) {
                String src = renameEvent.getSrcPath();
                String dst = renameEvent.getDstPath();

                if (InotifyPathFilter.matchesJobPrefix(dst, srcPrefix)) {
                    if (InotifyPathFilter.isStagingCommit(src, dst)) {
                        log.info("[Inotify Commit] Обнаружена фиксация данных из staging: '{}' -> '{}' (txid {})", src, dst, txid);
                        int created = handleStagingCommit(job, dst);
                        totalCreated += created;
                        orchestratorClient.updateJobStreamingTxid(job.getId(), txid, lag);
                    } else if (!InotifyPathFilter.isStagedPath(src) && !InotifyPathFilter.isStagedPath(dst)) {
                        log.info("[Inotify Rename] Переименование пути в постоянном каталоге: '{}' -> '{}' (txid {})", src, dst, txid);
                        int created = handleStagingCommit(job, dst);
                        totalCreated += created;
                        orchestratorClient.updateJobStreamingTxid(job.getId(), txid, lag);
                    }
                }
            } else if (event instanceof Event.UnlinkEvent unlinkEvent) {
                String path = unlinkEvent.getPath();
                if (InotifyPathFilter.matchesJobPrefix(path, srcPrefix) && !InotifyPathFilter.isStagedPath(path)) {
                    log.info("[Inotify Unlink] Удаление пути: '{}' (txid {})", path, txid);
                    if (job.isSyncDeletes()) {
                        // Регистрация удаления, если включена опция syncDeletes
                        enqueueDelete(job, path);
                    }
                    orchestratorClient.updateJobStreamingTxid(job.getId(), txid, lag);
                }
            }
        }

        return totalCreated;
    }

    /**
     * Обработка фиксации пути из staging.
     * Проверяет, является ли dstPath директорией (перемещение всей партиции) или файлом.
     */
    private int handleStagingCommit(JobDto job, String dstPath) {
        try {
            FileSystem fs = fsManager.getFileSystem();
            Path path = new Path(dstPath);

            if (!fs.exists(path)) {
                log.warn("[Inotify Commit] Путь '{}' не найден в HDFS (возможно, удален)", dstPath);
                return 0;
            }

            FileStatus status = fs.getFileStatus(path);
            if (status.isDirectory()) {
                // Случай перемещения каталога целиком (например, готовая партиция Hive/Spark)
                log.info("[Inotify Commit] Путь '{}' является директорией партиции. Рекурсивное сканирование файлов...", dstPath);
                List<TaskCreateItem> partitionTasks = new ArrayList<>();
                RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
                while (it.hasNext()) {
                    LocatedFileStatus file = it.next();
                    String filePath = file.getPath().toUri().getPath();
                    if (!InotifyPathFilter.isStagedPath(filePath)) {
                        String targetFilePath = computeTargetPath(job, filePath);
                        partitionTasks.add(new TaskCreateItem(filePath, targetFilePath, file.getLen(), false));
                    }
                }

                if (!partitionTasks.isEmpty()) {
                    boolean ok = orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(job.getId(), partitionTasks));
                    if (ok) {
                        log.info("[Inotify Commit] Успешно поставлено в очередь {} файлов партиции '{}'", partitionTasks.size(), dstPath);
                        return partitionTasks.size();
                    }
                }
            } else {
                // Одиночный файл
                return enqueueFile(job, dstPath, status.getLen());
            }
        } catch (IOException e) {
            log.error("[Inotify Commit Error] Ошибка проверки пути '{}': {}", dstPath, e.getMessage(), e);
        }
        return 0;
    }

    private int enqueueFile(JobDto job, String filePath, long fileSize) {
        String targetPath = computeTargetPath(job, filePath);
        TaskCreateItem item = new TaskCreateItem(filePath, targetPath, fileSize, false);
        boolean ok = orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(job.getId(), List.of(item)));
        return ok ? 1 : 0;
    }

    private void enqueueDelete(JobDto job, String deletedPath) {
        // Опциональная отправка удаления
        log.debug("[Inotify SyncDelete] Постановка задачи удаления: {}", deletedPath);
    }

    public static String computeTargetPath(JobDto job, String sourceFilePath) {
        String srcRoot = normalize(job.getSourcePath());
        String dstRoot = normalize(job.getTargetPath());
        String normalizedFile = normalize(sourceFilePath);

        if (normalizedFile.startsWith(srcRoot)) {
            String rel = normalizedFile.substring(srcRoot.length());
            if (rel.startsWith("/")) {
                rel = rel.substring(1);
            }
            return dstRoot + (dstRoot.endsWith("/") ? "" : "/") + rel;
        }

        return dstRoot + "/" + new Path(sourceFilePath).getName();
    }

    private static String normalize(String p) {
        if (p == null) return "";
        String s = p.trim();
        if (!s.startsWith("/")) s = "/" + s;
        if (s.endsWith("/") && s.length() > 1) s = s.substring(0, s.length() - 1);
        return s;
    }
}
