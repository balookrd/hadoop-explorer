package org.apache.hadoop.explorer.replicator.tool;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.hadoop.explorer.replicator.generated.control.AgentHeartbeatRequest;
import org.apache.hadoop.explorer.replicator.generated.control.AgentRegisterRequest;
import org.apache.hadoop.explorer.replicator.generated.control.GetPendingHmsJobsRequest;
import org.apache.hadoop.explorer.replicator.generated.control.HmsPendingJobItemProto;
import org.apache.hadoop.explorer.replicator.generated.control.OrchestratorControlPlaneServiceGrpc;
import org.apache.hadoop.explorer.replicator.generated.control.ReportHmsProgressRequest;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Рой эмулированных агентов для нагрузочной проверки оркестратора.
 *
 * <p>Каждый агент по настоящему протоколу регистрируется, шлет heartbeat, опрашивает HMS-задачи
 * своего кластера и отчитывается о прогрессе по полученным — так же часто, как реальный агент.
 * Агенты живут в отдельных кластерах ({@code swarm-src}/{@code swarm-dst}), чтобы не перехватывать
 * настоящие задачи стенда. На выходе — задержки и ошибки по каждому вызову и распределение задач
 * между агентами.
 *
 * <pre>
 * java -cp replicator-agent.jar org.apache.hadoop.explorer.replicator.tool.AgentSwarm \
 *     orchestrator:50050 &lt;agent-secret&gt; &lt;agents&gt; &lt;seconds&gt;
 * </pre>
 */
public final class AgentSwarm {

    private static final Map<String, ConcurrentLinkedQueue<Long>> LATENCIES = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> ERRORS = new ConcurrentHashMap<>();
    private static final Map<String, String> JOB_OWNER = new ConcurrentHashMap<>();

    private AgentSwarm() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Использование: AgentSwarm <host:port> <agent-secret> <agents> <seconds>");
            System.exit(1);
        }
        String target = args[0];
        String secret = args[1];
        int agents = Integer.parseInt(args[2]);
        long durationMs = Long.parseLong(args[3]) * 1000L;

        ManagedChannel channel = NettyChannelBuilder.forTarget(target)
                .usePlaintext()
                .intercept(AgentGrpcAuthInterceptor.createClientInterceptor(secret))
                .build();
        var stub = OrchestratorControlPlaneServiceGrpc.newBlockingStub(channel);

        long deadline = System.currentTimeMillis() + durationMs;
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < agents; i++) {
            // Половина роя — источники, половина — приемники: задаче нужен живой агент приемника
            String cluster = i % 2 == 0 ? "swarm-src" : "swarm-dst";
            String agentId = "swarm-agent-" + i;
            pool.submit(() -> runAgent(stub, agentId, cluster, deadline));
        }
        pool.shutdown();
        pool.awaitTermination(durationMs + 120_000, TimeUnit.MILLISECONDS);
        channel.shutdownNow();
        printReport(agents);
    }

    private static void runAgent(OrchestratorControlPlaneServiceGrpc.OrchestratorControlPlaneServiceBlockingStub stub,
                                 String agentId, String cluster, long deadline) {
        // IP-литерал, а не имя: проверка адреса в оркестраторе резолвит имена через DNS, и
        // несуществующие имена роя мерили бы таймауты резолвера стенда, а не оркестратор
        int n = Integer.parseInt(agentId.substring(agentId.lastIndexOf('-') + 1));
        String address = "10.250." + (n / 250) + "." + (n % 250 + 1) + ":50051";
        call("register", () -> stub.withDeadlineAfter(30, TimeUnit.SECONDS).registerAgent(AgentRegisterRequest.newBuilder()
                .setAgentId(agentId).setClusterId(cluster).setMode("all")
                .setAdvertisedGrpcAddress(address).setMaxBandwidthMbS(100).build()));
        long lastHeartbeat = 0;
        long eventId = 1;
        while (System.currentTimeMillis() < deadline) {
            long now = System.currentTimeMillis();
            if (now - lastHeartbeat >= 5000) {
                call("heartbeat", () -> stub.withDeadlineAfter(30, TimeUnit.SECONDS).heartbeat(AgentHeartbeatRequest.newBuilder()
                        .setAgentId(agentId).setClusterId(cluster).setAdvertisedGrpcAddress(address).setMode("all").build()));
                lastHeartbeat = now;
            }
            if ("swarm-src".equals(cluster)) {
                var resp = call("getPendingHmsJobs", () -> stub.withDeadlineAfter(30, TimeUnit.SECONDS).getPendingHmsJobs(
                        GetPendingHmsJobsRequest.newBuilder().setClusterId(cluster).setAgentId(agentId).build()));
                if (resp != null) {
                    for (HmsPendingJobItemProto job : resp.getJobsList()) {
                        JOB_OWNER.put(job.getJobId(), agentId);
                        long position = eventId++;
                        call("reportHmsProgress", () -> stub.withDeadlineAfter(30, TimeUnit.SECONDS).reportHmsProgress(
                                ReportHmsProgressRequest.newBuilder()
                                        .setJobId(job.getJobId()).setAgentId(agentId)
                                        .setStatus("QUEUED".equals(job.getStatus()) ? "BOOTSTRAPPING" : "ACTIVE")
                                        .setLastEventId(position)
                                        .setMessage("Синхронизировано " + position + " событий CDC. Текущее отставание: 0 событий.")
                                        .build()));
                    }
                }
            }
            sleepQuietly(2000 + ThreadLocalRandom.current().nextInt(1000));
        }
    }

    private interface Call<T> {
        T run();
    }

    private static <T> T call(String name, Call<T> call) {
        long started = System.nanoTime();
        try {
            T result = call.run();
            LATENCIES.computeIfAbsent(name, k -> new ConcurrentLinkedQueue<>()).add(System.nanoTime() - started);
            return result;
        } catch (Exception e) {
            ERRORS.computeIfAbsent(name + ": " + rootMessage(e), k -> new AtomicLong()).incrementAndGet();
            return null;
        }
    }

    private static String rootMessage(Throwable t) {
        String msg = t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
        return msg.length() > 120 ? msg.substring(0, 120) : msg;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printReport(int agents) {
        System.out.printf("Рой: %d агентов%n", agents);
        System.out.printf("%-20s %8s %9s %9s %9s %9s%n", "вызов", "кол-во", "p50 мс", "p95 мс", "p99 мс", "max мс");
        new TreeMap<>(LATENCIES).forEach((name, values) -> {
            List<Long> sorted = new ArrayList<>(values);
            Collections.sort(sorted);
            System.out.printf("%-20s %8d %9.1f %9.1f %9.1f %9.1f%n", name, sorted.size(),
                    pct(sorted, 50), pct(sorted, 95), pct(sorted, 99), sorted.get(sorted.size() - 1) / 1e6);
        });
        long errorTotal = ERRORS.values().stream().mapToLong(AtomicLong::get).sum();
        System.out.printf("Ошибок: %d%n", errorTotal);
        new TreeMap<>(ERRORS).forEach((k, v) -> System.out.printf("   %6d  %s%n", v.get(), k));
        Map<String, Integer> perAgent = new TreeMap<>();
        JOB_OWNER.values().forEach(a -> perAgent.merge(a, 1, Integer::sum));
        int min = perAgent.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        int max = perAgent.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        System.out.printf("Задач у агентов в конце: %d задач на %d агентах (min %d, max %d)%n",
                JOB_OWNER.size(), perAgent.size(), min, max);
        System.out.printf("SWARM_RESULT errors=%d jobs=%d owners=%d%n", errorTotal, JOB_OWNER.size(), perAgent.size());
    }

    private static double pct(List<Long> sorted, int p) {
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1);
        return sorted.get(Math.max(0, idx)) / 1e6;
    }
}
