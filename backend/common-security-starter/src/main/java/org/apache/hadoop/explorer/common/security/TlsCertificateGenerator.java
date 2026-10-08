package org.apache.hadoop.explorer.common.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Утилита для генерации самоподписанных TLS сертификатов и PKCS12 Keystore
 * для локального тестирования, CI и демонстрационных сред.
 */
public final class TlsCertificateGenerator {

    private static final Logger log = LoggerFactory.getLogger(TlsCertificateGenerator.class);
    private static volatile File cachedKeystoreFile;
    private static final String DEFAULT_PASSWORD = "changeit";

    private TlsCertificateGenerator() {}

    /**
     * Создает или возвращает закэшированный временный PKCS12 Keystore с сертификатом localhost.
     *
     * @param password пароль к хранилищу
     * @return путь к файлу PKCS12 Keystore
     */
    public static synchronized File getOrCreateSelfSignedKeystore(String password) throws IOException {
        if (cachedKeystoreFile != null && cachedKeystoreFile.exists()) {
            return cachedKeystoreFile;
        }

        String pass = (password != null && !password.isBlank()) ? password : DEFAULT_PASSWORD;
        Path tempDir = Files.createTempDirectory("hadoop-explorer-tls-");
        File keystoreFile = tempDir.resolve("keystore.p12").toFile();
        keystoreFile.deleteOnExit();
        tempDir.toFile().deleteOnExit();

        generateKeystoreWithKeytool(keystoreFile, pass);
        cachedKeystoreFile = keystoreFile;
        log.info("Создан временный самоподписанный PKCS12 Keystore: {}", keystoreFile.getAbsolutePath());
        return keystoreFile;
    }

    private static void generateKeystoreWithKeytool(File targetFile, String password) throws IOException {
        String javaHome = System.getProperty("java.home");
        String keytoolPath = "keytool";
        if (javaHome != null && !javaHome.isBlank()) {
            File binKeytool = new File(javaHome, "bin/keytool");
            if (binKeytool.exists() && binKeytool.canExecute()) {
                keytoolPath = binKeytool.getAbsolutePath();
            }
        }

        List<String> command = new ArrayList<>(List.of(
                keytoolPath,
                "-genkeypair",
                "-alias", "hadoop-explorer",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "365",
                "-keystore", targetFile.getAbsolutePath(),
                "-storetype", "PKCS12",
                "-storepass", password,
                "-keypass", password,
                "-dname", "CN=localhost, OU=Security, O=Hadoop Explorer, C=RU",
                "-ext", "SAN=dns:localhost,dns:orchestrator,dns:agent-dc1,dns:agent-dc2,ip:127.0.0.1"
        ));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        try {
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                String errorOutput = new String(process.getInputStream().readAllBytes());
                throw new IOException("Команда keytool завершилась с ошибкой (code " + exitCode + "): " + errorOutput);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Процесс генерации сертификата keytool был прерван", e);
        }
    }
}
