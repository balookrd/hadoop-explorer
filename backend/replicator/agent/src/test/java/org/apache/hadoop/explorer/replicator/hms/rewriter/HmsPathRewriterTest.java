package org.apache.hadoop.explorer.replicator.hms.rewriter;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HmsPathRewriterTest {

    @Test
    @DisplayName("Проверка fallback трансляции authority между кластерами hdfs-cluster-1 и hdfs-cluster-2")
    void testFallbackClusterAuthorityTranslation() {
        HmsPathRewriter rewriter = new HmsPathRewriter();

        String srcUri = "hdfs://hdfs-cluster-1:9000/apps/hive/warehouse/test_db.db/sales_report";
        var res = rewriter.rewrite(srcUri, "dc1", "dc2");

        assertEquals(srcUri, res.sourceUri());
        assertEquals("hdfs://hdfs-cluster-2:9000/apps/hive/warehouse/test_db.db/sales_report", res.targetUri());
        assertEquals("dc1", res.sourceClusterId());
        assertEquals("dc2", res.targetClusterId());
        assertFalse(res.unmapped());
    }

    @Test
    @DisplayName("Проверка обратной трансляции dc2 -> dc1")
    void testReverseFallbackTranslation() {
        HmsPathRewriter rewriter = new HmsPathRewriter();

        String srcUri = "hdfs://hdfs-cluster-2:9000/warehouse/db.db/t1";
        var res = rewriter.rewrite(srcUri, "dc2", "dc1");

        assertEquals("hdfs://hdfs-cluster-1:9000/warehouse/db.db/t1", res.targetUri());
        assertFalse(res.unmapped());
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
}
