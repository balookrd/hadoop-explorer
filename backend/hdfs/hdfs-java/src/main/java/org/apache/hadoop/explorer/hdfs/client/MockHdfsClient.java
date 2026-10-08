package org.apache.hadoop.explorer.hdfs.client;

import org.apache.hadoop.explorer.hdfs.dto.file.HdfsFileStatus;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.springframework.http.HttpStatus;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockHdfsClient implements HdfsFileSystemClient {

    public static class MockNode {
        public String path;
        public boolean isDirectory;
        public byte[] content = new byte[0];
        public String owner = "hdfs";
        public String group = "supergroup";
        public String permission = "755";
        public long modificationTime = System.currentTimeMillis();
        public long accessTime = System.currentTimeMillis();

        public MockNode(String path, boolean isDirectory) {
            this.path = normalize(path);
            this.isDirectory = isDirectory;
        }
    }

    private final ConcurrentMap<String, MockNode> nodes = new ConcurrentHashMap<>();

    public MockHdfsClient() {
        initDefaultNodes();
    }

    private void initDefaultNodes() {
        long now = System.currentTimeMillis();
        createDirInternal("/", "hdfs", "supergroup", "755", now);
        createDirInternal("/user", "hdfs", "supergroup", "755", now);
        createDirInternal("/tmp", "hdfs", "supergroup", "777", now);
        createDirInternal("/data", "hdfs", "supergroup", "755", now);
        createDirInternal("/data/warehouse", "hive", "hive", "775", now);

        // Демонстрационные файлы
        createFileInternal(
            "/data/sample.csv",
            "id,name,department,salary\n1,Иван Иванов,Аналитика,150000\n2,Петр Петров,Разработка,220000\n3,Анна Сидорова,Data Science,280000\n".getBytes(StandardCharsets.UTF_8),
            "hdfs", "supergroup", "644", now
        );
        createFileInternal(
            "/data/config.json",
            "{\n  \"cluster\": \"dev-cluster\",\n  \"active\": true,\n  \"nodes\": 5\n}".getBytes(StandardCharsets.UTF_8),
            "hdfs", "supergroup", "644", now
        );
        createFileInternal(
            "/data/README.md",
            "# HDFS Demo Directory\n\nДобро пожаловать в файловую систему HDFS Explorer!".getBytes(StandardCharsets.UTF_8),
            "hdfs", "supergroup", "644", now
        );
    }

    private static String normalize(String path) {
        if (path == null || path.isBlank()) return "/";
        String normalized = path.replace("\\", "/").replaceAll("/+", "/");
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private void ensureUserHome(String username) {
        if (username == null || username.isBlank()) return;
        String userDir = "/user/" + username;
        if (!nodes.containsKey(userDir)) {
            createDirInternal(userDir, username, "users", "700", System.currentTimeMillis());
        }
    }

    private void createDirInternal(String path, String owner, String group, String permission, long time) {
        String p = normalize(path);
        MockNode node = new MockNode(p, true);
        node.owner = owner;
        node.group = group;
        node.permission = permission;
        node.modificationTime = time;
        node.accessTime = time;
        nodes.put(p, node);
    }

    private void createFileInternal(String path, byte[] content, String owner, String group, String permission, long time) {
        String p = normalize(path);
        MockNode node = new MockNode(p, false);
        node.content = content != null ? content : new byte[0];
        node.owner = owner;
        node.group = group;
        node.permission = permission;
        node.modificationTime = time;
        node.accessTime = time;
        nodes.put(p, node);
    }

    @Override
    public List<HdfsFileStatus> listStatus(String path, String doAsUser) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        MockNode targetNode = nodes.get(target);
        if (targetNode == null) {
            throw new HdfsLocalizedException("Файл или директория не найдена: " + target, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }

        if (!targetNode.isDirectory) {
            return List.of(toStatus(targetNode, ""));
        }

        List<HdfsFileStatus> results = new ArrayList<>();
        String prefix = target.equals("/") ? "/" : target + "/";

        for (Map.Entry<String, MockNode> entry : nodes.entrySet()) {
            String p = entry.getKey();
            if (p.equals(target)) continue;

            if (p.startsWith(prefix)) {
                String sub = p.substring(prefix.length());
                if (!sub.contains("/")) { // непосредственный потомок
                    results.add(toStatus(entry.getValue(), sub));
                }
            }
        }

        results.sort(Comparator.comparing(HdfsFileStatus::getPathSuffix));
        return results;
    }

    @Override
    public HdfsFileStatus getFileStatus(String path, String doAsUser) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        MockNode node = nodes.get(target);
        if (node == null) {
            throw new HdfsLocalizedException("Файл или директория не найдена: " + target, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }
        int lastSlash = target.lastIndexOf('/');
        String suffix = (lastSlash >= 0 && lastSlash < target.length() - 1) ? target.substring(lastSlash + 1) : "";
        return toStatus(node, suffix);
    }

    @Override
    public InputStream open(String path, String doAsUser, long offset, Long length) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        MockNode node = nodes.get(target);
        if (node == null || node.isDirectory) {
            throw new HdfsLocalizedException("Файл не найден: " + target, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }

        byte[] all = node.content;
        int start = (int) Math.min(offset, all.length);
        int len = (length == null) ? (all.length - start) : (int) Math.min(length, all.length - start);
        len = Math.max(0, len);

        byte[] slice = new byte[len];
        System.arraycopy(all, start, slice, 0, len);
        return new ByteArrayInputStream(slice);
    }

    @Override
    public void create(String path, InputStream data, String doAsUser, boolean overwrite) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        MockNode existing = nodes.get(target);
        if (existing != null && existing.isDirectory) {
            throw new HdfsLocalizedException("Путь является директорией: " + target, HttpStatus.BAD_REQUEST, "FileAlreadyExistsException");
        }
        if (existing != null && !overwrite) {
            throw new HdfsLocalizedException("Файл уже существует: " + target, HttpStatus.CONFLICT, "FileAlreadyExistsException");
        }

        // Убедимся, что родительские директории созданы
        int lastSlash = target.lastIndexOf('/');
        if (lastSlash > 0) {
            String parent = target.substring(0, lastSlash);
            mkdirs(parent, doAsUser);
        }

        try {
            byte[] bytes = data.readAllBytes();
            createFileInternal(target, bytes, doAsUser != null ? doAsUser : "hdfs", "users", "644", System.currentTimeMillis());
        } catch (Exception e) {
            throw new RuntimeException("Ошибка записи данных: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean mkdirs(String path, String doAsUser) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        if (target.equals("/")) return true;

        String[] parts = target.split("/");
        StringBuilder current = new StringBuilder();
        long now = System.currentTimeMillis();

        for (String part : parts) {
            if (part.isBlank()) continue;
            current.append("/").append(part);
            String p = current.toString();
            MockNode existing = nodes.get(p);
            if (existing != null && !existing.isDirectory) {
                throw new HdfsLocalizedException("Невозможно создать директорию, путь занят файлом: " + p, HttpStatus.CONFLICT, "FileAlreadyExistsException");
            }
            if (existing == null) {
                createDirInternal(p, doAsUser != null ? doAsUser : "hdfs", "users", "755", now);
            }
        }
        return true;
    }

    @Override
    public boolean rename(String src, String dst, String doAsUser) {
        ensureUserHome(doAsUser);
        String s = normalize(src);
        String d = normalize(dst);

        MockNode srcNode = nodes.get(s);
        if (srcNode == null) {
            throw new HdfsLocalizedException("Исходный путь не найден: " + s, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }

        MockNode dstNode = nodes.get(d);
        if (dstNode != null && dstNode.isDirectory) {
            // Перемещение внутрь существующей директории
            String name = s.substring(s.lastIndexOf('/') + 1);
            d = normalize(d + "/" + name);
        }

        // Перемещаем сам узел
        nodes.remove(s);
        srcNode.path = d;
        nodes.put(d, srcNode);

        // Если это директория, обновляем всех потомков
        if (srcNode.isDirectory) {
            String oldPrefix = s + "/";
            String newPrefix = d + "/";
            for (Map.Entry<String, MockNode> entry : new HashMap<>(nodes).entrySet()) {
                if (entry.getKey().startsWith(oldPrefix)) {
                    String sub = entry.getKey().substring(oldPrefix.length());
                    String newChildPath = newPrefix + sub;
                    MockNode child = nodes.remove(entry.getKey());
                    if (child != null) {
                        child.path = newChildPath;
                        nodes.put(newChildPath, child);
                    }
                }
            }
        }
        return true;
    }

    @Override
    public boolean delete(String path, String doAsUser, boolean recursive) {
        ensureUserHome(doAsUser);
        String target = normalize(path);
        if (target.equals("/")) {
            throw new HdfsLocalizedException("Удаление корневой директории HDFS запрещено", HttpStatus.FORBIDDEN, "AccessControlException");
        }

        MockNode node = nodes.get(target);
        if (node == null) {
            throw new HdfsLocalizedException("Файл или директория не найдена: " + target, HttpStatus.NOT_FOUND, "FileNotFoundException");
        }

        if (node.isDirectory) {
            String prefix = target + "/";
            boolean hasChildren = nodes.keySet().stream().anyMatch(k -> k.startsWith(prefix));
            if (hasChildren && !recursive) {
                throw new HdfsLocalizedException("Каталог не пуст. Включите опцию рекурсивного удаления.", HttpStatus.CONFLICT, "PathIsNotEmptyDirectoryException");
            }
            if (recursive) {
                nodes.keySet().removeIf(k -> k.startsWith(prefix));
            }
        }

        nodes.remove(target);
        return true;
    }

    private HdfsFileStatus toStatus(MockNode node, String suffix) {
        int children = 0;
        if (node.isDirectory) {
            String prefix = node.path.equals("/") ? "/" : node.path + "/";
            for (String key : nodes.keySet()) {
                if (!key.equals(node.path) && key.startsWith(prefix)) {
                    String sub = key.substring(prefix.length());
                    if (!sub.contains("/")) children++;
                }
            }
        }

        return new HdfsFileStatus(
            suffix,
            node.isDirectory ? "DIRECTORY" : "FILE",
            node.isDirectory ? 0 : node.content.length,
            node.owner,
            node.group,
            node.permission,
            node.accessTime,
            node.modificationTime,
            134217728, // 128 MB
            node.isDirectory ? 0 : 3,
            children
        );
    }
}
