package io.github.balookrd.pristan.replicator.sender;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.balookrd.pristan.replicator.client.OrchestratorGrpcClient;
import io.github.balookrd.pristan.replicator.fs.HadoopFsManager;
import io.github.balookrd.pristan.replicator.generated.*;
import io.github.balookrd.pristan.replicator.model.CompleteTaskRequest;
import io.github.balookrd.pristan.replicator.model.FailTaskRequest;
import io.github.balookrd.pristan.replicator.model.TaskItemDto;
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

    /** Источник, каким его видит отправитель; по умолчанию удаленного пути на нем нет. */
    private final HadoopFsManager fsManager = mock(HadoopFsManager.class);

    /** Файлы каталога на приемнике; {@code null} — приемник не может отдать манифест. */
    private List<String> targetFiles = List.of();

    private static final String SOURCE_DIR = "/warehouse/sales/dt=2026-10-10";
    private static final String TARGET_DIR = "/backup/warehouse/sales/dt=2026-10-10";

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
                    public void getDirectoryManifest(DirectoryManifestRequest request,
                                                     StreamObserver<DirectoryManifestResponse> responseObserver) {
                        if (targetFiles == null) {
                            responseObserver.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
                            return;
                        }
                        DirectoryManifestResponse.Builder manifest = DirectoryManifestResponse.newBuilder().setExists(true);
                        targetFiles.forEach(f -> manifest.addFiles(FileManifestEntry.newBuilder().setRelativePath(f)));
                        responseObserver.onNext(manifest.build());
                        responseObserver.onCompleted();
                    }

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

    private static TaskItemDto deleteDirectoryTask(String targetAddress) {
        TaskItemDto task = deleteTask(targetAddress);
        task.setSourcePath(SOURCE_DIR);
        task.setTargetPath(TARGET_DIR);
        return task;
    }

    private static HadoopFsManager.FileItem sourceFile(String relativePath) {
        return new HadoopFsManager.FileItem(relativePath, SOURCE_DIR + "/" + relativePath, 1024L, 0L);
    }

    @Test
    @DisplayName("Файл, пересозданный на источнике, запоздавшее удаление не стирает")
    void testLateDeleteKeepsRecreatedFile() throws Exception {
        String target = startReceiver(true);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        TaskItemDto task = deleteTask("inprocess:" + target);
        when(fsManager.exists(eq(task.getSourcePath()), any(), anyBoolean())).thenReturn(true);
        when(fsManager.isDirectory(eq(task.getSourcePath()), any(), anyBoolean())).thenReturn(false);

        boolean result = senderFor(orchestrator).transferTask(task);

        assertTrue(result);
        assertTrue(deleteRequests.isEmpty(),
                "Путь снова есть на источнике: его содержимое переносит задача передачи файла, а удаление стерло бы свежие данные");
        verify(orchestrator).completeTask(any(CompleteTaskRequest.class));
    }

    @Test
    @DisplayName("Каталог, пересозданный на источнике: удаляются только файлы, которых на источнике нет")
    void testLateDeleteReconcilesRecreatedDirectory() throws Exception {
        String target = startReceiver(true);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(fsManager.exists(eq(SOURCE_DIR), any(), anyBoolean())).thenReturn(true);
        when(fsManager.isDirectory(eq(SOURCE_DIR), any(), anyBoolean())).thenReturn(true);
        when(fsManager.listFilesRecursively(eq(SOURCE_DIR), any(), anyBoolean()))
                .thenReturn(List.of(sourceFile("000000_0"), sourceFile("000001_0")));
        targetFiles = List.of("000000_0", "old/000007_0");

        boolean result = senderFor(orchestrator).transferTask(deleteDirectoryTask("inprocess:" + target));

        assertTrue(result);
        assertEquals(List.of(TARGET_DIR + "/old/000007_0"),
                deleteRequests.stream().map(DeletePathRequest::getPath).toList(),
                "Свежий файл 000000_0 остается, вытесненный перезаписью old/000007_0 удаляется");
        assertFalse(deleteRequests.get(0).getRecursive(), "Удаляется отдельный файл, а не каталог");
        verify(orchestrator).completeTask(any(CompleteTaskRequest.class));
    }

    @Test
    @DisplayName("Состояние источника неизвестно — на приемнике ничего не удаляется")
    void testUnknownSourceStateDeletesNothing() throws Exception {
        String target = startReceiver(true);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        TaskItemDto task = deleteTask("inprocess:" + target);
        when(fsManager.exists(eq(task.getSourcePath()), any(), anyBoolean()))
                .thenThrow(new IllegalStateException("NameNode недоступен"));

        boolean result = senderFor(orchestrator).transferTask(task);

        assertFalse(result);
        assertTrue(deleteRequests.isEmpty(), "«Не знаю» не равно «пути нет»");
        verify(orchestrator).failTask(any(FailTaskRequest.class));
        verify(orchestrator, never()).completeTask(any(CompleteTaskRequest.class));
    }

    @Test
    @DisplayName("Приемник не отдал список файлов пересозданного каталога — задача не закрывается успехом")
    void testReconcileWithoutTargetManifestFails() throws Exception {
        String target = startReceiver(true);
        OrchestratorGrpcClient orchestrator = mock(OrchestratorGrpcClient.class);
        when(fsManager.exists(eq(SOURCE_DIR), any(), anyBoolean())).thenReturn(true);
        when(fsManager.isDirectory(eq(SOURCE_DIR), any(), anyBoolean())).thenReturn(true);
        targetFiles = null;

        boolean result = senderFor(orchestrator).transferTask(deleteDirectoryTask("inprocess:" + target));

        assertFalse(result);
        assertTrue(deleteRequests.isEmpty());
        verify(orchestrator).failTask(any(FailTaskRequest.class));
    }
}
