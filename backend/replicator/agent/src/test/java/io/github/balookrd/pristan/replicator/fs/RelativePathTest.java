package io.github.balookrd.pristan.replicator.fs;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Relative path of a listed file under the job directory.
 *
 * <p>HDFS returns fully qualified paths from {@code listFiles}, the job directory usually comes
 * without a scheme. The prefix was compared as strings, never matched, and the fallback took the
 * bare file name: every file of a subdirectory landed in the root of the target directory, and
 * files with the same name from different partitions overwrote each other.
 */
@DisplayName("Относительный путь файла внутри каталога задания")
class RelativePathTest {

    @Test
    @DisplayName("Каталог без схемы, файл с полным URI: подкаталоги сохраняются")
    void unqualifiedBaseAndQualifiedFile() {
        assertEquals("dt=2026-10-10/part-0.parquet", HadoopFsManager.relativePath(
                new Path("/warehouse/sales"),
                new Path("hdfs://hdfs-cluster-1:9000/warehouse/sales/dt=2026-10-10/part-0.parquet")));
    }

    @Test
    @DisplayName("Каталог и файл с полным URI")
    void qualifiedBaseAndFile() {
        assertEquals("sub/a.txt", HadoopFsManager.relativePath(
                new Path("hdfs://ns1/data/in/"), new Path("hdfs://ns1/data/in/sub/a.txt")));
    }

    @Test
    @DisplayName("Файл в корне каталога задания")
    void fileDirectlyInBase() {
        assertEquals("a.txt", HadoopFsManager.relativePath(new Path("/data/in"), new Path("hdfs://ns1/data/in/a.txt")));
    }

    @Test
    @DisplayName("Каталог с именем-префиксом соседа не считается родителем")
    void siblingWithCommonPrefixIsNotParent() {
        assertEquals("a.txt", HadoopFsManager.relativePath(new Path("/data/in"), new Path("/data/in2/a.txt")));
    }
}
