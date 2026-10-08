package org.apache.hadoop.explorer.replicator.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * YARN Client для развертывания Replicator Agent в кластере Apache Hadoop YARN.
 *
 * <p>Упаковывает и отправляет ApplicationMaster в ResourceManager YARN,
 * который в свою очередь выделяет контейнеры на NodeManager'ах и запускает в них репликационные агенты.
 */
public class ReplicatorYarnClient {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorYarnClient.class);

    private final YarnConfiguration yarnConf;
    private final YarnClient yarnClient;

    public ReplicatorYarnClient() {
        this.yarnConf = new YarnConfiguration();
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(yarnConf);
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("j", "jar", true, "Путь к JAR-файлу Replicator Agent (обязательно)");
        options.addOption("n", "num_containers", true, "Количество контейнеров агентов (default: 1)");
        options.addOption("m", "memory", true, "Память контейнера в МБ (default: 1024)");
        options.addOption("v", "vcores", true, "Количество vCores (default: 1)");
        options.addOption("q", "queue", true, "Очередь YARN (default: default)");
        options.addOption("o", "orchestrator", true, "URL оркестратора (default: http://localhost:8005)");
        options.addOption("c", "cluster_id", true, "Идентификатор обслуживаемого HDFS кластера");
        options.addOption("h", "help", false, "Показать справку");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("help") || !cmd.hasOption("jar")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("yarn jar replicator-agent-java.jar " + ReplicatorYarnClient.class.getName() + " [options]", options);
                System.exit(0);
            }

            String jarPath = cmd.getOptionValue("jar");
            int numContainers = Integer.parseInt(cmd.getOptionValue("num_containers", "1"));
            int memoryMb = Integer.parseInt(cmd.getOptionValue("memory", "1024"));
            int vcores = Integer.parseInt(cmd.getOptionValue("vcores", "1"));
            String queue = cmd.getOptionValue("queue", "default");
            String orchestratorUrl = cmd.getOptionValue("orchestrator", "http://localhost:8005");
            String clusterId = cmd.getOptionValue("cluster_id", "demo-cluster");

            ReplicatorYarnClient client = new ReplicatorYarnClient();
            client.run(jarPath, numContainers, memoryMb, vcores, queue, orchestratorUrl, clusterId);

        } catch (Exception e) {
            System.err.println("Ошибка сабмита Replicator Agent в YARN: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run(String appJarPath, int numContainers, int containerMemoryMb, int containerVcores,
                    String queue, String orchestratorUrl, String clusterId) throws Exception {

        logger.info("Инициализация YARN Client...");
        yarnClient.start();

        logger.info("Запрос нового ApplicationId у ResourceManager...");
        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName("Hadoop-Replicator-Agent-" + clusterId);
        appContext.setQueue(queue);

        // 1. Копирование JAR в HDFS staging директорию YARN
        FileSystem fs = FileSystem.get(yarnConf);
        Path stagingDir = new Path(fs.getHomeDirectory(), ".replicator-staging/" + appId.toString());
        fs.mkdirs(stagingDir);

        Path srcJar = new Path(appJarPath);
        Path destJar = new Path(stagingDir, "replicator-agent-java.jar");
        logger.info("Копирование JAR в HDFS staging: {} -> {}", srcJar, destJar);
        fs.copyFromLocalFile(false, true, srcJar, destJar);
        FileStatus jarStatus = fs.getFileStatus(destJar);

        LocalResource appJarResource = Records.newRecord(LocalResource.class);
        appJarResource.setType(LocalResourceType.FILE);
        appJarResource.setVisibility(LocalResourceVisibility.APPLICATION);
        appJarResource.setResource(ConverterUtils.getYarnUrlFromPath(destJar));
        appJarResource.setTimestamp(jarStatus.getModificationTime());
        appJarResource.setSize(jarStatus.getLen());

        Map<String, LocalResource> localResources = new HashMap<>();
        localResources.put("replicator-agent-java.jar", appJarResource);

        // 2. Настройка окружения
        Map<String, String> env = new HashMap<>();
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(File.pathSeparatorChar).append("./*");
        for (String c : yarnConf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());
        env.put("ORCHESTRATOR_URL", orchestratorUrl);
        env.put("AGENT_CLUSTER_ID", clusterId);

        // 3. Командная строка запуска ApplicationMaster
        Vector<CharSequence> vargs = new Vector<>(30);
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx512m");
        vargs.add(ReplicatorApplicationMaster.class.getName());
        vargs.add("--num_containers");
        vargs.add(String.valueOf(numContainers));
        vargs.add("--container_memory");
        vargs.add(String.valueOf(containerMemoryMb));
        vargs.add("--container_vcores");
        vargs.add(String.valueOf(containerVcores));
        vargs.add("--orchestrator");
        vargs.add(orchestratorUrl);
        vargs.add("--cluster_id");
        vargs.add(clusterId);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = Collections.singletonList(command.toString());

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setLocalResources(localResources);
        amContainer.setEnvironment(env);
        amContainer.setCommands(commands);

        // 4. Ресурсы для ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(1);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        // 5. Отправка в YARN
        logger.info("Отправка ApplicationId {} в YARN ResourceManager...", appId);
        yarnClient.submitApplication(appContext);

        logger.info("======================================================================");
        logger.info("Replicator Agent успешно запущен в YARN! ApplicationId: {}", appId);
        logger.info("Для мониторинга используйте: yarn application -status {}", appId);
        logger.info("======================================================================");
    }
}
