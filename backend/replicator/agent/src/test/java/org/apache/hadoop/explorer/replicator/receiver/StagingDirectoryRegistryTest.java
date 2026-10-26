package org.apache.hadoop.explorer.replicator.receiver;

import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.shaper.LocalBandwidthLimiter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Набор каталогов для очистки осиротевших staging-файлов.
 *
 * <p>Каталог запоминался на каждый принятый файл и не вычищался никогда, а планировщик обходил
 * рекурсивно каждый накопленный каталог. На потоке мелких файлов это и утечка памяти приемника,
 * и шторм RPC к NameNode каждые несколько минут: обход тысяч каталогов, в которых давно ничего
 * не происходит.
 */
@DisplayName("SEC/PERF: набор каталогов для очистки ограничен")
class StagingDirectoryRegistryTest {

    private DataTransferServiceImpl service() throws IOException {
        String staging = Files.createTempDirectory("staging-registry").toFile().getCanonicalPath();
        return new DataTransferServiceImpl(staging, mock(HadoopFsManager.class), new LocalBandwidthLimiter(0.0));
    }

    @Test
    @DisplayName("Число запомненных каталогов не растет без предела")
    void knownDirectoriesShouldBeBounded() throws IOException {
        DataTransferServiceImpl service = service();

        for (int i = 0; i < DataTransferServiceImpl.MAX_KNOWN_TARGET_DIRECTORIES + 500; i++) {
            service.recordTargetDirectory("/warehouse/db" + i + "/part-0.parquet");
        }

        assertEquals(DataTransferServiceImpl.MAX_KNOWN_TARGET_DIRECTORIES,
                service.getKnownTargetDirectories().size(),
                "Набор каталогов должен быть ограничен сверху");
    }

    @Test
    @DisplayName("Вытесняются давно не встречавшиеся каталоги, свежие остаются")
    void recentDirectoriesShouldSurviveEviction() throws IOException {
        DataTransferServiceImpl service = service();

        service.recordTargetDirectory("/warehouse/hot/part-0.parquet");
        for (int i = 0; i < DataTransferServiceImpl.MAX_KNOWN_TARGET_DIRECTORIES - 1; i++) {
            service.recordTargetDirectory("/warehouse/cold" + i + "/part-0.parquet");
        }
        // Горячий каталог снова встретился — он не должен уйти при следующем вытеснении
        service.recordTargetDirectory("/warehouse/hot/part-1.parquet");
        service.recordTargetDirectory("/warehouse/newcomer/part-0.parquet");

        assertTrue(service.getKnownTargetDirectories().contains("/warehouse/hot"),
                "Каталог, в который только что писали, вытеснять нельзя");
        assertTrue(service.getKnownTargetDirectories().contains("/warehouse/newcomer"));
        assertFalse(service.getKnownTargetDirectories().contains("/warehouse/cold0"),
                "Самый давний каталог должен быть вытеснен");
    }

    @Test
    @DisplayName("Каталог запоминается один раз, повтор не раздувает набор")
    void repeatedDirectoryShouldNotGrowTheSet() throws IOException {
        DataTransferServiceImpl service = service();

        for (int i = 0; i < 100; i++) {
            service.recordTargetDirectory("/warehouse/sales/part-" + i + ".parquet");
        }

        assertEquals(1, service.getKnownTargetDirectories().size());
        assertTrue(service.getKnownTargetDirectories().contains("/warehouse/sales"));
    }
}
