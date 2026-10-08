package org.apache.hadoop.explorer.hdfs.service;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.explorer.hdfs.client.HdfsFileSystemClient;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterConfig;
import org.apache.hadoop.explorer.hdfs.dto.file.*;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class HdfsFileOperationService {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileOperationService.class);

    private final ClusterRegistry clusterRegistry;
    private final ClusterAclService aclService;
    private final FilePreviewService previewService;

    // upload_id -> set of received chunk indices
    private final ConcurrentMap<String, Set<Integer>> chunkTracker = new ConcurrentHashMap<>();

    public HdfsFileOperationService(ClusterRegistry clusterRegistry,
                                    ClusterAclService aclService,
                                    FilePreviewService previewService) {
        this.clusterRegistry = clusterRegistry;
        this.aclService = aclService;
        this.previewService = previewService;
    }

    public static String sanitizePath(String path) {
        if (path == null || path.isBlank()) return "/";
        String normalized = path.replace("\\", "/").replaceAll("/+", "/");
        if (normalized.contains("/../") || normalized.endsWith("/..") || normalized.startsWith("../")) {
            throw new HdfsLocalizedException("Недопустимый путь (Path Traversal): " + path, HttpStatus.BAD_REQUEST, "SecurityException");
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static String getParentPath(String path) {
        if (path == null || path.equals("/")) return null;
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) return "/";
        return path.substring(0, lastSlash);
    }

    public DirectoryListingResponse listFiles(String clusterId, String path, String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        if (!aclService.canAccessCluster(cluster, username, groups)) {
            throw new HdfsLocalizedException("Доступ к кластеру '" + clusterId + "' запрещен", HttpStatus.FORBIDDEN, "AccessControlException");
        }

        String safePath = sanitizePath(path);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        List<HdfsFileStatus> statuses;
        try {
            statuses = client.listStatus(safePath, username);
        } catch (HdfsLocalizedException e) {
            if ("FileNotFoundException".equalsIgnoreCase(e.getErrorClass()) && !"/".equals(safePath)) {
                try {
                    client.mkdirs(safePath, username);
                    statuses = client.listStatus(safePath, username);
                } catch (Exception ex) {
                    safePath = "/";
                    statuses = client.listStatus(safePath, username);
                }
            } else {
                throw e;
            }
        }

        int totalFiles = 0;
        int totalDirs = 0;
        long totalSize = 0;

        for (HdfsFileStatus s : statuses) {
            if ("DIRECTORY".equalsIgnoreCase(s.getType())) {
                totalDirs++;
            } else {
                totalFiles++;
                totalSize += s.getLength();
            }
        }

        boolean canWrite = !aclService.isClusterReadOnly(cluster, username, groups);

        return new DirectoryListingResponse(
            clusterId,
            safePath,
            getParentPath(safePath),
            statuses,
            totalFiles,
            totalDirs,
            totalSize,
            canWrite,
            true
        );
    }

    public FilePreviewResponse previewFile(String clusterId, String path, String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        if (!aclService.canAccessCluster(cluster, username, groups)) {
            throw new HdfsLocalizedException("Доступ к кластеру '" + clusterId + "' запрещен", HttpStatus.FORBIDDEN, "AccessControlException");
        }

        String safePath = sanitizePath(path);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        HdfsFileStatus status = client.getFileStatus(safePath, username);

        if ("DIRECTORY".equalsIgnoreCase(status.getType())) {
            throw new HdfsLocalizedException("Путь является директорией: " + safePath, HttpStatus.BAD_REQUEST, "NotAFileException");
        }

        int maxBytes = cluster.getPreviewMaxBytes() > 0 ? cluster.getPreviewMaxBytes() : 1048576;
        return previewService.preview(client, clusterId, safePath, username, status.getLength(), maxBytes);
    }


    public FileActionResponse uploadFile(String clusterId, String targetDir, MultipartFile file,
                                         String relativePath, boolean overwrite,
                                         String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safeDir = sanitizePath(targetDir);
        String filename = file.getOriginalFilename();
        if (filename == null || filename.isBlank()) {
            throw new HdfsLocalizedException("Имя файла не может быть пустым", HttpStatus.BAD_REQUEST);
        }

        String finalPath;
        if (relativePath != null && !relativePath.isBlank()) {
            String rel = sanitizePath(relativePath);
            if (rel.startsWith("/")) rel = rel.substring(1);
            finalPath = sanitizePath(safeDir + "/" + rel);
        } else {
            finalPath = sanitizePath(safeDir + "/" + filename);
        }

        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        try (InputStream in = file.getInputStream()) {
            client.create(finalPath, in, username, overwrite);
            log.info("File uploaded: cluster={}, path={}, user={}", clusterId, finalPath, username);
            return FileActionResponse.ok("Файл успешно загружен", finalPath);
        } catch (IOException e) {
            log.error("Failed to upload file to {}", finalPath, e);
            throw new HdfsLocalizedException("Ошибка загрузки файла: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public FileActionResponse uploadChunk(String clusterId, String uploadId, String targetDir,
                                          String filename, int chunkIndex, int totalChunks,
                                          MultipartFile file, boolean overwrite,
                                          String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safeDir = sanitizePath(targetDir);
        Path uploadTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "hdfs-uploads", uploadId);

        try {
            Files.createDirectories(uploadTempDir);
            Path chunkFile = uploadTempDir.resolve("part-" + chunkIndex);
            file.transferTo(chunkFile.toFile());

            Set<Integer> chunks = chunkTracker.computeIfAbsent(uploadId, id -> ConcurrentHashMap.newKeySet());
            chunks.add(chunkIndex);

            if (chunks.size() == totalChunks) {
                // Сборка полного файла
                String finalPath = sanitizePath(safeDir + "/" + filename);
                Path assembled = uploadTempDir.resolve("complete.tmp");

                try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(assembled))) {
                    for (int i = 0; i < totalChunks; i++) {
                        Path part = uploadTempDir.resolve("part-" + i);
                        if (!Files.exists(part)) {
                            throw new HdfsLocalizedException("Отсутствует чанк #" + i, HttpStatus.BAD_REQUEST);
                        }
                        Files.copy(part, out);
                    }
                }

                // Запись в HDFS
                HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
                try (InputStream in = new BufferedInputStream(Files.newInputStream(assembled))) {
                    client.create(finalPath, in, username, overwrite);
                }

                // Очистка
                deleteDirectoryRecursively(uploadTempDir.toFile());
                chunkTracker.remove(uploadId);

                log.info("Chunked upload completed: cluster={}, path={}, chunks={}", clusterId, finalPath, totalChunks);
                return FileActionResponse.ok("Многокомпонентная загрузка успешно завершена", finalPath);
            }

            return FileActionResponse.ok(String.format("Чанк %d/%d получен", chunkIndex + 1, totalChunks));
        } catch (IOException e) {
            log.error("Chunk upload failed for uploadId {}", uploadId, e);
            throw new HdfsLocalizedException("Ошибка чанковой загрузки: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public ChunkedUploadStatusResponse getChunkUploadStatus(String clusterId, String uploadId, int totalChunks) {
        Set<Integer> received = chunkTracker.getOrDefault(uploadId, Collections.emptySet());
        List<Integer> list = new ArrayList<>(received);
        Collections.sort(list);
        return new ChunkedUploadStatusResponse(uploadId, list, totalChunks, list.size() == totalChunks);
    }

    public FileActionResponse uploadArchive(String clusterId, String targetDir, MultipartFile file,
                                            boolean overwrite, String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safeDir = sanitizePath(targetDir);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        int extractedCount = 0;

        try (ZipArchiveInputStream zipIn = new ZipArchiveInputStream(file.getInputStream())) {
            ZipArchiveEntry entry;
            while ((entry = zipIn.getNextZipEntry()) != null) {
                String name = entry.getName();
                if (name.contains("..")) continue; // Path traversal guard

                String fullPath = sanitizePath(safeDir + "/" + name);
                if (entry.isDirectory()) {
                    client.mkdirs(fullPath, username);
                } else {
                    client.create(fullPath, zipIn, username, overwrite);
                    extractedCount++;
                }
            }
            return FileActionResponse.ok("Архив успешно распакован. Извлечено файлов: " + extractedCount, safeDir);
        } catch (IOException e) {
            log.error("Archive extraction failed for {}", safeDir, e);
            throw new HdfsLocalizedException("Ошибка распаковки архива: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public FileActionResponse makeDirectory(String clusterId, String path, String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safePath = sanitizePath(path);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        client.mkdirs(safePath, username);
        return FileActionResponse.ok("Директория создана", safePath);
    }

    public FileActionResponse renamePath(String clusterId, String src, String dst, String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safeSrc = sanitizePath(src);
        String safeDst = sanitizePath(dst);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        client.rename(safeSrc, safeDst, username);
        return FileActionResponse.ok("Путь переименован", safeDst);
    }

    public FileActionResponse deletePath(String clusterId, String path, boolean recursive,
                                         String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        String safePath = sanitizePath(path);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        client.delete(safePath, username, recursive);
        return FileActionResponse.ok("Успешно удалено", safePath);
    }

    public BatchDeleteResponse batchDelete(String clusterId, List<String> paths, boolean recursive,
                                           String username, Collection<String> groups) {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        aclService.validateWriteAccess(cluster, username, groups);

        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        List<String> deleted = new ArrayList<>();
        List<Map<String, String>> failed = new ArrayList<>();

        for (String p : paths) {
            try {
                String safe = sanitizePath(p);
                client.delete(safe, username, recursive);
                deleted.add(safe);
            } catch (Exception e) {
                failed.add(Map.of("path", p, "error", e.getMessage() != null ? e.getMessage() : "Unknown error"));
            }
        }

        return new BatchDeleteResponse(deleted, failed, paths.size(), failed.isEmpty());
    }

    public void streamDownload(String clusterId, String path, String username, Collection<String> groups,
                               OutputStream out) throws IOException {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        if (!aclService.canAccessCluster(cluster, username, groups)) {
            throw new HdfsLocalizedException("Доступ к кластеру '" + clusterId + "' запрещен", HttpStatus.FORBIDDEN);
        }

        String safePath = sanitizePath(path);
        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        HdfsFileStatus status = client.getFileStatus(safePath, username);

        if ("DIRECTORY".equalsIgnoreCase(status.getType())) {
            // Скачивание каталога в виде ZIP стрима
            try (ZipOutputStream zos = new ZipOutputStream(out)) {
                zipDirectory(client, safePath, "", username, zos);
            }
        } else {
            try (InputStream in = client.open(safePath, username, 0, null)) {
                in.transferTo(out);
            }
        }
    }

    public void streamBatchDownload(String clusterId, List<String> paths, String username,
                                    Collection<String> groups, OutputStream out) throws IOException {
        ClusterConfig cluster = clusterRegistry.getClusterOrThrow(clusterId);
        if (!aclService.canAccessCluster(cluster, username, groups)) {
            throw new HdfsLocalizedException("Доступ к кластеру '" + clusterId + "' запрещен", HttpStatus.FORBIDDEN);
        }

        HdfsFileSystemClient client = clusterRegistry.getClient(clusterId);
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            for (String p : paths) {
                String safe = sanitizePath(p);
                HdfsFileStatus st = client.getFileStatus(safe, username);
                String baseName = safe.substring(safe.lastIndexOf('/') + 1);

                if ("DIRECTORY".equalsIgnoreCase(st.getType())) {
                    zipDirectory(client, safe, baseName, username, zos);
                } else {
                    zos.putNextEntry(new ZipEntry(baseName));
                    try (InputStream in = client.open(safe, username, 0, null)) {
                        in.transferTo(zos);
                    }
                    zos.closeEntry();
                }
            }
        }
    }

    private void zipDirectory(HdfsFileSystemClient client, String dirPath, String zipBase,
                              String username, ZipOutputStream zos) throws IOException {
        List<HdfsFileStatus> children = client.listStatus(dirPath, username);
        for (HdfsFileStatus ch : children) {
            String childName = ch.getPathSuffix();
            String fullChildPath = dirPath.equals("/") ? "/" + childName : dirPath + "/" + childName;
            String zipEntryPath = zipBase.isBlank() ? childName : zipBase + "/" + childName;

            if ("DIRECTORY".equalsIgnoreCase(ch.getType())) {
                zipDirectory(client, fullChildPath, zipEntryPath, username, zos);
            } else {
                zos.putNextEntry(new ZipEntry(zipEntryPath));
                try (InputStream in = client.open(fullChildPath, username, 0, null)) {
                    in.transferTo(zos);
                }
                zos.closeEntry();
            }
        }
    }

    public CrossClusterCopyResponse crossClusterCopy(CrossClusterCopyRequest req, String username, Collection<String> groups) {
        ClusterConfig srcCluster = clusterRegistry.getClusterOrThrow(req.sourceClusterId());
        ClusterConfig dstCluster = clusterRegistry.getClusterOrThrow(req.targetClusterId());

        if (!aclService.canAccessCluster(srcCluster, username, groups)) {
            throw new HdfsLocalizedException("Нет доступа к исходному кластеру", HttpStatus.FORBIDDEN);
        }
        aclService.validateWriteAccess(dstCluster, username, groups);

        String srcPath = sanitizePath(req.sourcePath());
        String dstPath = sanitizePath(req.targetPath());

        HdfsFileSystemClient srcClient = clusterRegistry.getClient(req.sourceClusterId());
        HdfsFileSystemClient dstClient = clusterRegistry.getClient(req.targetClusterId());

        HdfsFileStatus srcStatus = srcClient.getFileStatus(srcPath, username);

        int[] copiedFiles = new int[]{0};
        long[] copiedBytes = new long[]{0};

        if ("DIRECTORY".equalsIgnoreCase(srcStatus.getType())) {
            copyDirectoryRecursive(srcClient, srcPath, dstClient, dstPath, username, req.overwrite(), copiedFiles, copiedBytes);
        } else {
            copySingleFile(srcClient, srcPath, dstClient, dstPath, username, req.overwrite(), copiedFiles, copiedBytes);
        }

        return new CrossClusterCopyResponse(
            true,
            String.format("Скопировано файлов: %d, объем: %d байт", copiedFiles[0], copiedBytes[0]),
            req.sourceClusterId(), srcPath,
            req.targetClusterId(), dstPath,
            copiedFiles[0], copiedBytes[0]
        );
    }

    private void copySingleFile(HdfsFileSystemClient src, String srcP,
                                HdfsFileSystemClient dst, String dstP,
                                String user, boolean overwrite,
                                int[] files, long[] bytes) {
        HdfsFileStatus st = src.getFileStatus(srcP, user);
        try (InputStream in = src.open(srcP, user, 0, null)) {
            dst.create(dstP, in, user, overwrite);
            files[0]++;
            bytes[0] += st.getLength();
        } catch (Exception e) {
            throw new RuntimeException("Ошибка копирования файла " + srcP + ": " + e.getMessage(), e);
        }
    }

    private void copyDirectoryRecursive(HdfsFileSystemClient src, String srcP,
                                        HdfsFileSystemClient dst, String dstP,
                                        String user, boolean overwrite,
                                        int[] files, long[] bytes) {
        dst.mkdirs(dstP, user);
        List<HdfsFileStatus> list = src.listStatus(srcP, user);
        for (HdfsFileStatus item : list) {
            String s = srcP.equals("/") ? "/" + item.getPathSuffix() : srcP + "/" + item.getPathSuffix();
            String d = dstP.equals("/") ? "/" + item.getPathSuffix() : dstP + "/" + item.getPathSuffix();
            if ("DIRECTORY".equalsIgnoreCase(item.getType())) {
                copyDirectoryRecursive(src, s, dst, d, user, overwrite, files, bytes);
            } else {
                copySingleFile(src, s, dst, d, user, overwrite, files, bytes);
            }
        }
    }

    private static void deleteDirectoryRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) deleteDirectoryRecursively(f);
            }
        }
        file.delete();
    }
}
