package org.apache.hadoop.explorer.hdfs.controller;

import org.apache.hadoop.explorer.hdfs.dto.file.*;
import org.apache.hadoop.explorer.hdfs.service.HdfsFileOperationService;
import org.apache.hadoop.explorer.hdfs.util.SecurityUtils;
import org.apache.hadoop.explorer.common.audit.Audited;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/files")
public class FileController {

    private final HdfsFileOperationService fileService;

    public FileController(HdfsFileOperationService fileService) {
        this.fileService = fileService;
    }

    @GetMapping
    public ResponseEntity<DirectoryListingResponse> listFiles(
            @PathVariable String clusterId,
            @RequestParam(defaultValue = "/") String path,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.listFiles(clusterId, path, user, groups));
    }

    @GetMapping("/preview")
    public ResponseEntity<FilePreviewResponse> previewFile(
            @PathVariable String clusterId,
            @RequestParam String path,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.previewFile(clusterId, path, user, groups));
    }

    @GetMapping("/download")
    public ResponseEntity<StreamingResponseBody> downloadFile(
            @PathVariable String clusterId,
            @RequestParam String path,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);

        String safePath = HdfsFileOperationService.sanitizePath(path);
        String filename = safePath.substring(safePath.lastIndexOf('/') + 1);
        if (filename.isBlank()) filename = "download";

        String encodedName = URLEncoder.encode(filename, StandardCharsets.UTF_8).replace("+", "%20");

        StreamingResponseBody body = out -> fileService.streamDownload(clusterId, safePath, user, groups, out);

        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + encodedName + "\"")
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(body);
    }

    @PostMapping("/upload")
    @Audited(action = "FILE_UPLOAD", resource = "HDFS")
    public ResponseEntity<FileActionResponse> uploadFile(
            @PathVariable String clusterId,
            @RequestParam String path,
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "relative_path", required = false) String relativePath,
            @RequestParam(defaultValue = "true") boolean overwrite,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        FileActionResponse resp = fileService.uploadFile(clusterId, path, file, relativePath, overwrite, user, groups);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/upload-chunk")
    public ResponseEntity<FileActionResponse> uploadChunk(
            @PathVariable String clusterId,
            @RequestParam("upload_id") String uploadId,
            @RequestParam String path,
            @RequestParam String filename,
            @RequestParam("chunk_index") int chunkIndex,
            @RequestParam("total_chunks") int totalChunks,
            @RequestParam("file") MultipartFile file,
            @RequestParam(defaultValue = "true") boolean overwrite,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        FileActionResponse resp = fileService.uploadChunk(
            clusterId, uploadId, path, filename, chunkIndex, totalChunks, file, overwrite, user, groups);
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/upload-chunk/status")
    public ResponseEntity<ChunkedUploadStatusResponse> getChunkUploadStatus(
            @PathVariable String clusterId,
            @RequestParam("upload_id") String uploadId,
            @RequestParam("total_chunks") int totalChunks) {
        return ResponseEntity.ok(fileService.getChunkUploadStatus(clusterId, uploadId, totalChunks));
    }

    @PostMapping("/upload-archive")
    @Audited(action = "ARCHIVE_UPLOAD", resource = "HDFS")
    public ResponseEntity<FileActionResponse> uploadArchive(
            @PathVariable String clusterId,
            @RequestParam String path,
            @RequestParam("file") MultipartFile file,
            @RequestParam(defaultValue = "true") boolean overwrite,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        FileActionResponse resp = fileService.uploadArchive(clusterId, path, file, overwrite, user, groups);
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/mkdir")
    @Audited(action = "DIRECTORY_CREATE", resource = "HDFS")
    public ResponseEntity<FileActionResponse> makeDirectory(
            @PathVariable String clusterId,
            @RequestParam String path,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.makeDirectory(clusterId, path, user, groups));
    }

    @PostMapping("/rename")
    @Audited(action = "PATH_RENAME", resource = "HDFS")
    public ResponseEntity<FileActionResponse> renamePath(
            @PathVariable String clusterId,
            @RequestParam String src,
            @RequestParam String dst,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.renamePath(clusterId, src, dst, user, groups));
    }

    @DeleteMapping("/delete")
    @Audited(action = "PATH_DELETE", resource = "HDFS")
    public ResponseEntity<FileActionResponse> deletePath(
            @PathVariable String clusterId,
            @RequestParam String path,
            @RequestParam(defaultValue = "false") boolean recursive,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.deletePath(clusterId, path, recursive, user, groups));
    }

    @PostMapping("/batch-delete")
    @Audited(action = "BATCH_DELETE", resource = "HDFS")
    public ResponseEntity<BatchDeleteResponse> batchDelete(
            @PathVariable String clusterId,
            @RequestBody BatchDeleteRequest req,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);
        return ResponseEntity.ok(fileService.batchDelete(clusterId, req.paths(), req.recursive(), user, groups));
    }

    @PostMapping("/batch-download")
    public ResponseEntity<StreamingResponseBody> batchDownload(
            @PathVariable String clusterId,
            @RequestBody BatchDownloadRequest req,
            Authentication auth) {
        String user = SecurityUtils.getUsername(auth);
        List<String> groups = SecurityUtils.getGroups(auth);

        StreamingResponseBody body = out -> fileService.streamBatchDownload(clusterId, req.paths(), user, groups, out);

        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"batch_download.zip\"")
            .contentType(MediaType.parseMediaType("application/zip"))
            .body(body);
    }
}
