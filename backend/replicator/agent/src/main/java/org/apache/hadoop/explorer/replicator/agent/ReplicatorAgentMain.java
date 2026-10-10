package org.apache.hadoop.explorer.replicator.agent;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Точка входа для запуска Replicator Agent на нодах Hadoop (DataNode, Edge Node, Standalone Daemon).
 */
public class ReplicatorAgentMain {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorAgentMain.class);

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("id", "agent-id", true, "Уникальный идентификатор агента");
        options.addOption("c", "cluster-id", true, "Идентификатор обслуживаемого HDFS-кластера");
        options.addOption("m", "mode", true, "Режим работы (all, sender, receiver)");
        options.addOption("o", "orchestrator", true, "URL оркестратора (http://host:port)");
        options.addOption("p", "port", true, "Порт gRPC приемника (default: 50051)");
        options.addOption("b", "bandwidth", true, "Локальный лимит полосы пропускания в МБ/с");
        options.addOption("s", "staging-dir", true, "Директория временных файлов staging");
        options.addOption("z", "compression", true, "Кодек потокового сжатия в канале (zstd, lz4, none; default: zstd)");
        options.addOption("zl", "compression-level", true, "Уровень сжатия Zstd (1-22; default: 3)");
        options.addOption("h", "help", false, "Показать справку");

        CommandLineParser parser = new DefaultParser();
        ReplicatorAgentConfig config = ReplicatorAgentConfig.fromEnv();

        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("replicator-agent [options]", options);
                System.exit(0);
            }

            if (cmd.hasOption("agent-id")) config.setAgentId(cmd.getOptionValue("agent-id"));
            if (cmd.hasOption("cluster-id")) config.setClusterId(cmd.getOptionValue("cluster-id"));
            if (cmd.hasOption("mode")) config.setMode(cmd.getOptionValue("mode"));
            if (cmd.hasOption("orchestrator")) config.setOrchestratorUrl(cmd.getOptionValue("orchestrator"));
            if (cmd.hasOption("port")) config.setReceiverPort(Integer.parseInt(cmd.getOptionValue("port")));
            if (cmd.hasOption("bandwidth")) config.setMaxBandwidthMbS(Double.parseDouble(cmd.getOptionValue("bandwidth")));
            if (cmd.hasOption("staging-dir")) config.setStagingDir(cmd.getOptionValue("staging-dir"));
            if (cmd.hasOption("compression")) {
                config.setWireCompressionCodec(org.apache.hadoop.explorer.replicator.compression.WireCompressor.parseCodec(cmd.getOptionValue("compression")));
            }
            if (cmd.hasOption("compression-level")) {
                config.setWireCompressionLevel(Integer.parseInt(cmd.getOptionValue("compression-level")));
            }

        } catch (ParseException e) {
            System.err.println("Ошибка разбора аргументов командной строки: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("replicator-agent [options]", options);
            System.exit(1);
        }

        ReplicatorAgent agent = new ReplicatorAgent(config);
        CountDownLatch stopLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Перехвачен сигнал завершения (SIGTERM/SIGINT), остановка агента...");
            agent.stop();
            stopLatch.countDown();
        }, "replicator-shutdown-hook"));

        try {
            agent.start();
            logger.info("Replicator Agent запущен и готов к работе. Нажмите Ctrl+C для остановки.");
            stopLatch.await();
        } catch (Exception e) {
            logger.error("Критическая ошибка запуска Replicator Agent: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
