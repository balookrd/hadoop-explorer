package org.apache.hadoop.explorer.replicator.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Сервис рекурсивного сканирования директорий HDFS, сетевого сравнения манифестов
 * и построения оптимизированного распределенного пула пофайловых задач (Analyzer Engine).
 */
public class DirectoryDiffEngine {

    private static final Logger logger = LoggerFactory.getLogger(DirectoryDiffEngine.class);

    private final HadoopFsManager fsManager;
    private final OrchestratorGrpcClient orchestratorClient;
    private final ObjectMapper objectMapper;
    private final long smallFileThresholdBytes;
    private final long bundleTargetSizeBytes;
    private final int maxBundleFiles;

    public DirectoryDiffEngine(HadoopFsManager fsManager,
                               OrchestratorGrpcClient orchestratorClient,
                               ObjectMapper objectMapper,
                               long smallFileThresholdBytes,
                               long bundleTargetSizeBytes,
                               int maxBundleFiles) {
        this.fsManager = fsManager;
        this.orchestratorClient = orchestratorClient;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.smallFileThresholdBytes = smallFileThresholdBytes;
        this.bundleTargetSizeBytes = bundleTargetSizeBytes;
        this.maxBundleFiles = maxBundleFiles;
    }

    /**
     * Анализ каталога или одиночного файла и формирование пула подзадач в Оркестраторе.
     */
    public boolean analyzeAndCreateTaskPool(JobDto job, ManagedChannel channel) {
        String jobId = job.getId();
        String sourcePath = job.getSourcePath();
        String targetPath = job.getTargetPath();

        boolean asService = job.getRunAsServiceAccount() != null && job.getRunAsServiceAccount();
        if (!fsManager.exists(sourcePath, job.getExecutionPrincipal(), asService)) {
            String err = "Исходный путь не найден: " + sourcePath;
            logger.error(err);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }

        try {
            // Pre-flight очистка старых staging-файлов предыдущей попытки этого задания
            try {
                fsManager.cleanStagingFiles(targetPath, jobId, 0L, job.getExecutionPrincipal(), asService);
            } catch (Exception e) {
                logger.debug("Предстартовая очистка staging файлов для job_id={}: {}", jobId, e.getMessage());
            }

            boolean isDir = fsManager.isDirectory(sourcePath, job.getExecutionPrincipal(), asService);
            List<TaskCreateItem> taskItems = new ArrayList<>();

            if (isDir) {
                if (HadoopFsManager.isIgnoredDirectoryPath(sourcePath)) {
                    logger.info("Каталог {} пропущен согласно фильтру временных папок", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                List<HadoopFsManager.FileItem> items = fsManager.listFilesRecursively(
                        sourcePath, job.getExecutionPrincipal(), asService
                );

                if (items.isEmpty()) {
                    logger.info("Каталог {} пуст или все файлы отфильтрованы. Создание пустой задачи.", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                // Запрос пакетного манифеста у целевого агента (1 сетевой round-trip)
                DirectoryManifestResponse remoteManifest = fetchRemoteManifest(channel, targetPath, job);
                Map<String, Long> remoteFileSizes = new HashMap<>();
                if (remoteManifest != null && remoteManifest.getExists()) {
                    for (FileManifestEntry entry : remoteManifest.getFilesList()) {
                        remoteFileSizes.put(entry.getRelativePath(), entry.getSize());
                    }
                }

                // In-memory diff и формирование пула подзадач с бандлингом мелких файлов
                List<HadoopFsManager.FileItem> currentBundle = new ArrayList<>();
                long currentBundleBytes = 0;

                for (HadoopFsManager.FileItem item : items) {
                    Long remoteSize = remoteFileSizes.get(item.relativePath());
                    boolean skipped = (remoteSize != null && remoteSize == item.size() && item.size() > 0);
                    String subTargetPath = combinePaths(targetPath, item.relativePath());

                    if (skipped) {
                        taskItems.add(new TaskCreateItem(
                                UUID.randomUUID().toString(),
                                item.fullPath(),
                                subTargetPath,
                                item.size(),
                                true,
                                null,
                                "FILE",
                                1,
                                null
                        ));
                        continue;
                    }

                    // Если файл >= порога мелких файлов или бандлинг отключен (порог <= 0)
                    if (smallFileThresholdBytes <= 0 || item.size() >= smallFileThresholdBytes) {
                        taskItems.add(new TaskCreateItem(
                                UUID.randomUUID().toString(),
                                item.fullPath(),
                                subTargetPath,
                                item.size(),
                                false,
                                null,
                                "FILE",
                                1,
                                null
                        ));
                    } else {
                        // Мелкий файл: добавляем в накапливаемый бандл
                        currentBundle.add(item);
                        currentBundleBytes += item.size();

                        if (currentBundle.size() >= maxBundleFiles || currentBundleBytes >= bundleTargetSizeBytes) {
                            taskItems.add(createBundleTaskItem(sourcePath, targetPath, currentBundle, currentBundleBytes));
                            currentBundle = new ArrayList<>();
                            currentBundleBytes = 0;
                        }
                    }
                }

                // Завершающий бандл (если остались файлы)
                if (!currentBundle.isEmpty()) {
                    taskItems.add(createBundleTaskItem(sourcePath, targetPath, currentBundle, currentBundleBytes));
                }
            } else {
                if (HadoopFsManager.isIgnoredPath(sourcePath)) {
                    logger.info("Файл {} пропущен согласно фильтру временных файлов", sourcePath);
                    orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, Collections.emptyList()));
                    return true;
                }

                long fileBytes = fsManager.getFileSize(sourcePath, job.getExecutionPrincipal(), asService);
                CheckFileResponse check = checkRemoteFile(channel, targetPath, job);
                boolean skipped = (check != null && check.getExists() && check.getSize() == fileBytes && fileBytes > 0);

                taskItems.add(new TaskCreateItem(
                        UUID.randomUUID().toString(),
                        sourcePath,
                        targetPath,
                        fileBytes,
                        skipped
                ));
            }

            boolean ok = orchestratorClient.batchCreateTasks(new BatchCreateTasksRequest(jobId, taskItems));
            logger.info("Для задания '{}' успешно сформирован и зарегистрирован пул из {} пофайловых задач", jobId, taskItems.size());
            return ok;
        } catch (Exception e) {
            String err = "Ошибка при анализе каталога " + sourcePath + ": " + e.getMessage();
            logger.error(err, e);
            orchestratorClient.updateJobProgress(jobId, new UpdateJobRequest("FAILED", 0L, 0L, err));
            return false;
        }
    }

    public DirectoryManifestResponse fetchRemoteManifest(ManagedChannel channel, String targetDir, JobDto job) {
        if (channel == null) return null;
        try {
            DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub =
                    DataTransferServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);

            DirectoryManifestRequest req = DirectoryManifestRequest.newBuilder()
                    .setPath(targetDir)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "")
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .build();

            return blockingStub.getDirectoryManifest(req);
        } catch (Exception e) {
            logger.debug("Не удалось получить пакетный манифест каталога '{}' через gRPC: {}", targetDir, e.getMessage());
            return null;
        }
    }

    public CheckFileResponse checkRemoteFile(ManagedChannel channel, String targetPath, JobDto job) {
        if (channel == null) return null;
        try {
            DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub =
                    DataTransferServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);

            CheckFileRequest req = CheckFileRequest.newBuilder()
                    .setPath(targetPath)
                    .setExecutionPrincipal(job.getExecutionPrincipal() != null ? job.getExecutionPrincipal() : "")
                    .setRunAsServiceAccount(job.getRunAsServiceAccount() != null ? job.getRunAsServiceAccount() : false)
                    .build();

            return blockingStub.checkFile(req);
        } catch (Exception e) {
            logger.debug("Целевой узел недоступен или файл '{}' еще не существует: {}", targetPath, e.getMessage());
            return null;
        }
    }

    public TaskCreateItem createBundleTaskItem(String baseSourcePath, String baseTargetPath,
                                               List<HadoopFsManager.FileItem> bundleFiles, long totalBytes) {
        List<ReplicationSender.BundleFileEntry> entries = new ArrayList<>(bundleFiles.size());
        for (HadoopFsManager.FileItem item : bundleFiles) {
            entries.add(new ReplicationSender.BundleFileEntry(item.fullPath(), item.relativePath(), item.size()));
        }
        String manifestJson = "";
        try {
            manifestJson = objectMapper.writeValueAsString(entries);
        } catch (Exception e) {
            logger.error("Ошибка сериализации bundleManifest: {}", e.getMessage());
        }
        return new TaskCreateItem(
                UUID.randomUUID().toString(),
                baseSourcePath,
                baseTargetPath,
                totalBytes,
                false,
                null,
                "BUNDLE_TAR",
                bundleFiles.size(),
                manifestJson
        );
    }

    private static String combinePaths(String base, String rel) {
        if (base == null || base.isEmpty()) return rel;
        if (rel == null || rel.isEmpty()) return base;
        boolean baseEnds = base.endsWith("/");
        boolean relStarts = rel.startsWith("/");
        if (baseEnds && relStarts) {
            return base + rel.substring(1);
        } else if (!baseEnds && !relStarts) {
            return base + "/" + rel;
        } else {
            return base + rel;
        }
    }
}
