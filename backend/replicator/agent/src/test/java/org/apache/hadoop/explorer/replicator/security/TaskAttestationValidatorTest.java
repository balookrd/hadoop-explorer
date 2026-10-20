package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.explorer.replicator.generated.FileMetadata;
import org.apache.hadoop.explorer.replicator.generated.TarStreamMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TaskAttestationValidator Unit Tests")
class TaskAttestationValidatorTest {

    private static final String SECRET = "secret-key-1234567890-test-key";

    @Test
    @DisplayName("Должен успешно валидировать подлинный токен аттестации файла")
    void shouldValidateGenuineFileAttestationToken() {
        TaskAttestationValidator validator = new TaskAttestationValidator(SECRET, true);

        String token = TaskAttestationService.generateToken(
                "task-1", "job-1", "/target/file.txt", "hdfs_user", false, SECRET
        );

        FileMetadata metadata = FileMetadata.newBuilder()
                .setTaskId("task-1")
                .setJobId("job-1")
                .setTargetPath("/target/file.txt")
                .setExecutionPrincipal("hdfs_user")
                .setRunAsServiceAccount(false)
                .setTaskToken(token)
                .build();

        assertDoesNotThrow(() -> validator.validateTaskAttestation(metadata));
    }

    @Test
    @DisplayName("Должен успешно валидировать подлинный токен аттестации TAR-бандла")
    void shouldValidateGenuineTarStreamAttestationToken() {
        TaskAttestationValidator validator = new TaskAttestationValidator(SECRET, true);

        String token = TaskAttestationService.generateToken(
                "tar-task-1", "tar-job-1", "/target/bundle", "hdfs_user", true, SECRET
        );

        TarStreamMetadata metadata = TarStreamMetadata.newBuilder()
                .setTaskId("tar-task-1")
                .setJobId("tar-job-1")
                .setBaseTargetPath("/target/bundle")
                .setExecutionPrincipal("hdfs_user")
                .setRunAsServiceAccount(true)
                .setTaskToken(token)
                .build();

        assertDoesNotThrow(() -> validator.validateTaskAttestation(metadata));
    }

    @Test
    @DisplayName("Должен выбрасывать SecurityException при компрометации параметров задачи")
    void shouldRejectTamperedTaskToken() {
        TaskAttestationValidator validator = new TaskAttestationValidator(SECRET, true);

        String validToken = TaskAttestationService.generateToken(
                "task-1", "job-1", "/target/file.txt", "hdfs_user", false, SECRET
        );

        // Злоумышленник подменил путь назначения targetPath
        FileMetadata tamperedMetadata = FileMetadata.newBuilder()
                .setTaskId("task-1")
                .setJobId("job-1")
                .setTargetPath("/etc/passwd")
                .setExecutionPrincipal("hdfs_user")
                .setRunAsServiceAccount(false)
                .setTaskToken(validToken)
                .build();

        SecurityException ex = assertThrows(SecurityException.class, () ->
                validator.validateTaskAttestation(tamperedMetadata)
        );
        assertTrue(ex.getMessage().contains("Криптографическая аттестация задачи не пройдена"));
    }

    @Test
    @DisplayName("В режиме enforceTaskAttestation=true отклонять запросы без токена")
    void shouldRejectMissingTokenWhenEnforced() {
        TaskAttestationValidator validator = new TaskAttestationValidator(SECRET, true);

        FileMetadata metadataWithoutToken = FileMetadata.newBuilder()
                .setTaskId("task-1")
                .setJobId("job-1")
                .setTargetPath("/target/file.txt")
                .build();

        SecurityException ex = assertThrows(SecurityException.class, () ->
                validator.validateTaskAttestation(metadataWithoutToken)
        );
        assertTrue(ex.getMessage().contains("требуется криптографическая аттестация"));
    }

    @Test
    @DisplayName("В режиме enforceTaskAttestation=false пропускать запросы без токена (обратная совместимость)")
    void shouldAllowMissingTokenWhenNotEnforced() {
        TaskAttestationValidator validator = new TaskAttestationValidator(SECRET, false);

        FileMetadata metadataWithoutToken = FileMetadata.newBuilder()
                .setTaskId("task-1")
                .setJobId("job-1")
                .setTargetPath("/target/file.txt")
                .build();

        assertDoesNotThrow(() -> validator.validateTaskAttestation(metadataWithoutToken));
    }
}
