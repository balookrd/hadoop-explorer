package org.apache.hadoop.explorer.replicator.orchestrator.grpc;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.security.AgentGrpcAuthInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Управление жизненным циклом (SmartLifecycle) gRPC сервера Control Plane в Оркестраторе.
 *
 * <p>Поддерживает выделенный пул платформенных потоков (предотвращающий Carrier Thread Pinning
 * при транзакциях JPA / H2), L7-аутентификацию по X-Agent-Secret и конфигурируемый TLS.
 */
@Component
public class OrchestratorGrpcServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorGrpcServer.class);

    private final ReplicatorProperties properties;
    private final OrchestratorControlPlaneServiceImpl service;

    private Server grpcServer;
    private ExecutorService grpcExecutor;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public OrchestratorGrpcServer(ReplicatorProperties properties, OrchestratorControlPlaneServiceImpl service) {
        this.properties = properties;
        this.service = service;
    }

    @Override
    public void start() {
        var grpcConfig = properties.getGrpc();
        if (grpcConfig == null || !grpcConfig.isEnabled()) {
            log.info("gRPC Control Plane отключен в конфигурации");
            return;
        }

        int port = grpcConfig.getPort();
        log.info("Инициализация Orchestrator gRPC Control Plane на порту {}...", port);

        int poolSize = Math.max(16, Runtime.getRuntime().availableProcessors() * 4);
        this.grpcExecutor = new ThreadPoolExecutor(
                poolSize,
                Math.max(64, poolSize * 2),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "grpc-orchestrator-" + counter.getAndIncrement());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        try {
            NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                    .executor(grpcExecutor)
                    .addService(service)
                    .maxInboundMessageSize(grpcConfig.getMaxInboundMessageSize())
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(10, TimeUnit.SECONDS);

            // Аутентификация по X-Agent-Secret
            String secret = properties.getAgentSecret();
            if (secret != null && !secret.isBlank()) {
                builder.intercept(new AgentGrpcAuthInterceptor(secret));
                log.info("Включена gRPC L7 аутентификация по X-Agent-Secret в Control Plane");
            }

            // TLS конфигурация (опционально)
            if (grpcConfig.isTlsEnabled()) {
                SslContextBuilder sslBuilder;
                if (grpcConfig.getCertChainPath() != null && grpcConfig.getPrivateKeyPath() != null) {
                    sslBuilder = SslContextBuilder.forServer(
                            new File(grpcConfig.getCertChainPath()),
                            new File(grpcConfig.getPrivateKeyPath())
                    );
                } else {
                    log.info("gRPC TLS включен без путей к сертификатам: генерация временного SelfSignedCertificate");
                    SelfSignedCertificate ssc = new SelfSignedCertificate("localhost");
                    sslBuilder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
                }

                if (grpcConfig.getTrustCertCollectionPath() != null && !grpcConfig.getTrustCertCollectionPath().isBlank()) {
                    sslBuilder.trustManager(new File(grpcConfig.getTrustCertCollectionPath()));
                }

                String clientAuth = grpcConfig.getClientAuth();
                if ("REQUIRE".equalsIgnoreCase(clientAuth) || "REQUIRED".equalsIgnoreCase(clientAuth)) {
                    sslBuilder.clientAuth(ClientAuth.REQUIRE);
                } else if ("OPTIONAL".equalsIgnoreCase(clientAuth)) {
                    sslBuilder.clientAuth(ClientAuth.OPTIONAL);
                } else {
                    sslBuilder.clientAuth(ClientAuth.NONE);
                }

                builder.sslContext(GrpcSslContexts.configure(sslBuilder).build());
                log.info("gRPC TLS активирован на Control Plane (mTLS: {})", clientAuth);
            }

            this.grpcServer = builder.build().start();
            isRunning.set(true);
            log.info("==========================================================================");
            log.info("🚀 Orchestrator gRPC Control Plane успешно запущен и слушает порт {}", grpcServer.getPort());
            log.info("==========================================================================");

        } catch (IOException e) {
            log.error("КРИТИЧЕСКАЯ ОШИБКА старта Orchestrator gRPC Control Plane: {}", e.getMessage(), e);
            throw new IllegalStateException("Не удалось запустить gRPC Control Plane на порту " + port, e);
        } catch (Exception e) {
            log.error("Сбой инициализации TLS для gRPC сервера: {}", e.getMessage(), e);
            throw new IllegalStateException("Сбой TLS для gRPC Control Plane", e);
        }
    }

    @Override
    public void stop() {
        if (grpcServer != null) {
            log.info("Остановка Orchestrator gRPC Control Plane...");
            try {
                grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Принудительная остановка gRPC сервера (Interrupted)");
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                if (grpcExecutor != null) {
                    grpcExecutor.shutdown();
                }
                isRunning.set(false);
                log.info("Orchestrator gRPC Control Plane остановлен");
            }
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public int getPhase() {
        // Запуск после подъема JPA и core сервисов
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    public int getPort() {
        return grpcServer != null ? grpcServer.getPort() : properties.getGrpc().getPort();
    }
}
