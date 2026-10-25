package org.apache.hadoop.explorer.replicator.sender;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.explorer.replicator.client.OrchestratorGrpcClient;
import org.apache.hadoop.explorer.replicator.fs.HadoopFsManager;
import org.apache.hadoop.explorer.replicator.generated.*;
import org.apache.hadoop.explorer.replicator.model.CompleteTaskRequest;
import org.apache.hadoop.explorer.replicator.model.FailTaskRequest;
import org.apache.hadoop.explorer.replicator.model.TaskItemDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Удаление, пришедшее из inotify, должно доходить до приемника.
 *
 * <p>`enqueueDelete` в обработчике inotify был пустым, и при включенном `syncDeletes` удаления не
 * реплицировались никогда: файл, удаленный на источнике, оставался на приемнике навсегда.
 * Постановка задачи — только половина пути; вторая половина здесь: воркер обязан исполнить задачу
 * типа `DELETE` вызовом удаления на приемнике, а не передачей байт отсутствующего файла.
 */
@DisplayName("Репликация удалений: исполнение задачи DELETE воркером")
class DeleteTaskReplicationTest {

    private Server server;

    private final List<DeletePathRequest> deleteRequests = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    /** Приемник, фиксирующий запросы удаления. */
    private String startReceiver(boolean succeed) throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new DataTransferServiceGrpc.DataTransferServiceImplBase() {
                    @Override
                    public void deletePath(DeletePathRequest request,
                                           StreamObserver<DeletePathResponse> responseObserver) {
                        deleteRequests.add(request);
                        responseObserver.onNext(DeletePathResponse.newBuilder()
                                .setJobId(request.getJobId())
                                .setSuccess(succeed)
                                .setMessage(succeed ? "удалено" : "путь занят")
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        return name;
    }

    private ReplicationSender senderFor(OrchestratorGrpcClient orchestrator) {
        HadoopFsManager fsManager = mock(HadoopFsManager.class);
        return new ReplicationSender(
                "worker-delete-1",
                orchestrator,
                fsManager,
                null,
                32 * 1024,
                false, null, null, null, false,
                1024 * 1024L, 16 * 1024 * 1024L, 500
        );
    }

    private static TaskItemDto deleteTask(String targetAddress) {
        TaskItemDto task = new TaskItemDto();
        task.setId("task-delete-1");
        task.setJobId("job-streaming-1");
        task.setSourcePath("/warehouse/sales/dt=2026-10-10/part-0.parquet");
        task.setTargetPath("/backup/warehouse/sales/dt=2026-10-10/part-0.parquet");
        task.setTaskType("DELETE");
        task.setFileSize(0);
        task.setTargetAddress(targetAddress);
        return task;
    }

    @Test
    @DisplayName("Воркер удаляет путь на приемнике и закрывает задачу")
    void testDeleteTaskRemovesPathOnReceiver() throws Exception {
        String target = startReceiver(true);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(orchestrator.completeTask(any(CompleteTaskRequest.class))).thenReturn(true);

        boolean result = senderFor(orchestrator).transferTask(deleteTask("inprocess:" + target));

        assertTrue(result, "Задача удаления обязана выполниться");
        assertEquals(1, deleteRequests.size(),
                "Приемник не получил запрос удаления: удаление на источнике осталось без эффекта");
        assertEquals("/backup/warehouse/sales/dt=2026-10-10/part-0.parquet", deleteRequests.get(0).getPath(),
                "Удалять нужно путь приемника, а не путь источника");
        assertTrue(deleteRequests.get(0).getRecursive(),
                "Удаление каталога партиции должно быть рекурсивным");
        verify(orchestrator).completeTask(any(CompleteTaskRequest.class));
        verify(orchestrator, never()).failTask(any(FailTaskRequest.class));
    }

    @Test
    @DisplayName("Отказ приемника в удалении не выдается за успех")
    void testFailedDeleteIsReportedAsFailure() throws Exception {
        String target = startReceiver(false);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);

        boolean result = senderFor(orchestrator).transferTask(deleteTask("inprocess:" + target));

        assertFalse(result, "Неуспешное удаление нельзя считать выполненной задачей");
        ArgumentCaptor<FailTaskRequest> captor = ArgumentCaptor.forClass(FailTaskRequest.class);
        verify(orchestrator).failTask(captor.capture());
        assertTrue(captor.getValue().getErrorMessage() != null
                        && !captor.getValue().getErrorMessage().isBlank(),
                "Причина отказа обязана попасть в отчет: иначе оператор не узнает, что путь остался");
        verify(orchestrator, never()).completeTask(any(CompleteTaskRequest.class));
    }
}
