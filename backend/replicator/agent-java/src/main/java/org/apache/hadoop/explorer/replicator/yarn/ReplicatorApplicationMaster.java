package org.apache.hadoop.explorer.replicator.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ApplicationMaster сервиса Replicator Agent в Apache Hadoop YARN.
 *
 * <p>Управляет пулом контейнеров агентов репликации на узлах кластера (NodeManager),
 * динамически запрашивая ресурсы у ResourceManager и запуская экземпляры {@link ReplicatorAgentMain}.
 */
public class ReplicatorApplicationMaster implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatorApplicationMaster.class);

    private final YarnConfiguration yarnConf;
    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private NMClientAsync nmClientAsync;

    private int numContainers = 1;
    private int containerMemory = 1024;
    private int containerVcores = 1;
    private String orchestratorUrl = "http://localhost:8005";
    private String clusterId = "demo-cluster";

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final Map<ContainerId, Container> runningContainers = new ConcurrentHashMap<>();

    public ReplicatorApplicationMaster() {
        this.yarnConf = new YarnConfiguration();
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("n", "num_containers", true, "Количество контейнеров");
        options.addOption("m", "container_memory", true, "Память контейнера в МБ");
        options.addOption("v", "container_vcores", true, "Количество vCores");
        options.addOption("o", "orchestrator", true, "URL оркестратора");
        options.addOption("c", "cluster_id", true, "Идентификатор кластера");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            ReplicatorApplicationMaster am = new ReplicatorApplicationMaster();

            if (cmd.hasOption("num_containers")) am.numContainers = Integer.parseInt(cmd.getOptionValue("num_containers"));
            if (cmd.hasOption("container_memory")) am.containerMemory = Integer.parseInt(cmd.getOptionValue("container_memory"));
            if (cmd.hasOption("container_vcores")) am.containerVcores = Integer.parseInt(cmd.getOptionValue("container_vcores"));
            if (cmd.hasOption("orchestrator")) am.orchestratorUrl = cmd.getOptionValue("orchestrator");
            if (cmd.hasOption("cluster_id")) am.clusterId = cmd.getOptionValue("cluster_id");

            am.run();
        } catch (Exception e) {
            logger.error("Критическая ошибка работы Replicator ApplicationMaster: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    public void run() throws Exception {
        logger.info("Старт Replicator ApplicationMaster (numContainers={}, memory={}MB, vcores={}, clusterId={})...",
                numContainers, containerMemory, containerVcores, clusterId);

        // 1. Инициализация клиентов YARN
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, this);
        amRMClient.init(yarnConf);
        amRMClient.start();

        nmClientAsync = NMClientAsync.createNMClientAsync(this);
        nmClientAsync.init(yarnConf);
        nmClientAsync.start();

        // 2. Регистрация AM в ResourceManager
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster("", 0, "");
        logger.info("ApplicationMaster успешно зарегистрирован в ResourceManager");

        // 3. Запрос контейнеров у ResourceManager
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(containerVcores);

        for (int i = 0; i < numContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(
                    capability, null, null, pri, true);
            amRMClient.addContainerRequest(containerAsk);
        }
        logger.info("Запрошено {} контейнеров у YARN ResourceManager", numContainers);

        // 4. Ожидание завершения или сигнала остановки
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanup, "am-shutdown-hook"));

        while (isRunning.get()) {
            Thread.sleep(3000);
        }

        cleanup();
    }

    private synchronized void cleanup() {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }

        logger.info("Завершение работы ApplicationMaster, освобождение контейнеров...");
        try {
            for (Container container : runningContainers.values()) {
                nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
            }

            if (amRMClient != null) {
                amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Replicator AM остановлен", null);
                amRMClient.stop();
            }
            if (nmClientAsync != null) {
                nmClientAsync.stop();
            }
        } catch (Exception e) {
            logger.warn("Ошибка при завершении ApplicationMaster: {}", e.getMessage());
        }
    }

    // ========================================================================
    // AMRMClientAsync Callbacks
    // ========================================================================

    @Override
    public void onContainersAllocated(List<Container> containers) {
        logger.info("ResourceManager выделил {} новых контейнеров", containers.size());

        for (Container container : containers) {
            runningContainers.put(container.getId(), container);

            try {
                // Подготовка команды запуска ReplicatorAgentMain в контейнере YARN
                Vector<CharSequence> vargs = new Vector<>(30);
                vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
                vargs.add("-Xmx" + (containerMemory - 128) + "m");
                vargs.add(ReplicatorAgentMain.class.getName());
                vargs.add("--agent-id");
                vargs.add("yarn-" + container.getId().toString());
                vargs.add("--cluster-id");
                vargs.add(clusterId);
                vargs.add("--orchestrator");
                vargs.add(orchestratorUrl);
                vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/replicator.stdout");
                vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/replicator.stderr");

                StringBuilder command = new StringBuilder();
                for (CharSequence str : vargs) {
                    command.append(str).append(" ");
                }

                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(command.toString()));

                Map<String, String> env = new HashMap<>();
                env.put("CLASSPATH", "./replicator-agent-java.jar:" + System.getenv("CLASSPATH"));
                env.put("ORCHESTRATOR_URL", orchestratorUrl);
                env.put("AGENT_CLUSTER_ID", clusterId);
                ctx.setEnvironment(env);

                logger.info("Запуск Replicator Agent в контейнере {} на узле {}",
                        container.getId(), container.getNodeId());
                nmClientAsync.startContainerAsync(container, ctx);

            } catch (Exception e) {
                logger.error("Не удалось запустить контейнер {}: {}", container.getId(), e.getMessage(), e);
            }
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            logger.info("Контейнер {} завершился со статусом: exit_code={}",
                    status.getContainerId(), status.getExitStatus());
            runningContainers.remove(status.getContainerId());
        }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
        return 0.5f;
    }

    @Override
    public void onError(Throwable e) {
        logger.error("Ошибка в канале связи с ResourceManager: {}", e.getMessage(), e);
        cleanup();
    }

    @Override
    public void onShutdownRequest() {
        logger.info("Получен запрос на завершение от ResourceManager");
        cleanup();
    }

    // ========================================================================
    // NMClientAsync Callbacks
    // ========================================================================

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, java.nio.ByteBuffer> allServiceResponse) {
        logger.info("Контейнер {} успешно запущен на NodeManager", containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {}

    @Override
    public void onContainerStopped(ContainerId containerId) {
        logger.info("Контейнер {} остановлен", containerId);
        runningContainers.remove(containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        logger.error("Ошибка старта контейнера {}: {}", containerId, t.getMessage());
        runningContainers.remove(containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {}

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {}
}
