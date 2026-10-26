package org.apache.hadoop.explorer.replicator.hms.rewriter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HmsPathRewriterTest {

    @Test
    @DisplayName("Проверка динамической трансляции authority по границам идентификаторов кластеров")
    void testDynamicClusterAuthorityTranslation() {
        HmsPathRewriter rewriter = new HmsPathRewriter();

        // 1. Сопоставление с портом
        String srcUri = "hdfs://prod-nn1.corp:9000/apps/hive/warehouse/test_db.db/sales_report";
        var res = rewriter.rewrite(srcUri, "prod-nn1", "dr-nn2");

        assertEquals(srcUri, res.sourceUri());
        assertEquals("hdfs://dr-nn2.corp:9000/apps/hive/warehouse/test_db.db/sales_report", res.targetUri());
        assertEquals("prod-nn1", res.sourceClusterId());
        assertEquals("dr-nn2", res.targetClusterId());
        assertFalse(res.unmapped());

        // 2. Обратная трансляция
        String reverseUri = "hdfs://dc2:8020/warehouse/db.db/t1";
        var reverseRes = rewriter.rewrite(reverseUri, "dc2", "dc1");
        assertEquals("hdfs://dc1:8020/warehouse/db.db/t1", reverseRes.targetUri());
        assertFalse(reverseRes.unmapped());
    }

    @Test
    @DisplayName("Проверка сохранения оригинального пути при отсутствии сопоставления (unmapped)")
    void testUnmappedWhenNoRuleMatches() {
        HmsPathRewriter rewriter = new HmsPathRewriter();

        String unknownUri = "hdfs://external-shared-storage:8020/warehouse/data";
        var res = rewriter.rewrite(unknownUri, "dc1", "dc2");

        assertEquals(unknownUri, res.sourceUri());
        assertEquals(unknownUri, res.targetUri(), "Путь не должен искажаться при отсутствии сопоставления");
        assertTrue(res.unmapped(), "Должен быть выставлен флаг unmapped");
    }

    @Test
    @DisplayName("Проверка явных FederationMappings")
    void testFederationMappings() {
        List<HmsPathRewriter.FederationMapping> mappings = List.of(
                new HmsPathRewriter.FederationMapping("ns-hot", "hdfs://ns-target:8020"),
                new HmsPathRewriter.FederationMapping("ns-cold", "ns-target-cold:8020")
        );
        HmsPathRewriter rewriter = new HmsPathRewriter(mappings);

        var res1 = rewriter.rewrite("hdfs://ns-hot/warehouse/t1", "dc1", "dc2");
        assertEquals("hdfs://ns-target:8020/warehouse/t1", res1.targetUri());

        var res2 = rewriter.rewrite("hdfs://ns-cold/warehouse/t2", "dc1", "dc2");
        assertEquals("hdfs://ns-target-cold:8020/warehouse/t2", res2.targetUri());
    }

    @Test
    @DisplayName("Проверка targetDefaultFs")
    void testTargetDefaultFs() {
        HmsPathRewriter rewriter = new HmsPathRewriter(List.of(), "hdfs://target-namenode:8020");

        var res = rewriter.rewrite("hdfs://some-source:9000/data/warehouse/tbl", "dc1", "dc2");
        assertEquals("hdfs://target-namenode:8020/data/warehouse/tbl", res.targetUri());

        var resRel = rewriter.rewrite("/data/warehouse/tbl", "dc1", "dc2");
        assertEquals("hdfs://target-namenode:8020/data/warehouse/tbl", resRel.targetUri());
    }

    @Test
    @DisplayName("Парсинг строки маппингов из переменной окружения")
    void testParseMappings() {
        String input = "ns1=hdfs://ns2:8020, cluster-1:9000->cluster-2:9000; ns-cold=ns-warm";
        var list = HmsPathRewriter.parseMappings(input);

        assertEquals(3, list.size());
        assertEquals("ns1", list.get(0).sourceNameservice());
        assertEquals("hdfs://ns2:8020", list.get(0).targetNameservice());
        assertEquals("cluster-1:9000", list.get(1).sourceNameservice());
        assertEquals("cluster-2:9000", list.get(1).targetNameservice());
        assertEquals("ns-cold", list.get(2).sourceNameservice());
        assertEquals("ns-warm", list.get(2).targetNameservice());
    }

    @Test
    @DisplayName("Экранирование метастора (%3A) и пробелы в значениях партиций переносятся как есть")
    void escapedAndSpacedPartitionPathsSurvive() {
        HmsPathRewriter rewriter = new HmsPathRewriter(
                HmsPathRewriter.parseMappings("hdfs-cluster-1:9000=hdfs://hdfs-cluster-2:9000"), null);

        var escaped = rewriter.rewrite("hdfs://hdfs-cluster-1:9000/wh/t/ts=2026-10-01 10%3A00%3A00", "dc1", "dc2");
        assertEquals("hdfs://hdfs-cluster-2:9000/wh/t/ts=2026-10-01 10%3A00%3A00", escaped.targetUri(),
                "путь приемника должен совпадать с каталогом, который создаст метастор");
        assertFalse(escaped.unmapped(), "пробел в значении партиции не должен ронять перезапись");
    }
}
