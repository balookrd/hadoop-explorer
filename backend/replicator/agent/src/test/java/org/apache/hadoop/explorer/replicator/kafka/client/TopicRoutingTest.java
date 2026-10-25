package org.apache.hadoop.explorer.replicator.kafka.client;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Маршрутизация батчей по задаче репликации.
 *
 * <p>Агент склеивал топики и целевые эндпоинты всех активных задач ЦОД в один пул, после чего
 * HRW-балансировщик раскидывал партиции одного топика по всем приемникам без разбора. Если в
 * ЦОД живут две задачи с разными целевыми кластерами (например, прямая DC1→DC2 и обратное
 * направление BIDIRECTIONAL-задачи DC1→DC3), часть данных уезжала в чужой кластер, а в нужный
 * не доезжала вовсе.
 */
@DisplayName("Kafka: маршрутизация топиков по целевым эндпоинтам задачи")
class TopicRoutingTest {

    private static ManagedChannel channel(String name) {
        return InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static KafkaReplicationClient clientWithThreeTargets() {
        return new KafkaReplicationClient(
                List.of(channel("dc2-a"), channel("dc2-b"), channel("dc3-a")),
                List.of("kafka-dc2-a:50052", "kafka-dc2-b:50052", "kafka-dc3-a:50052"));
    }

    @Test
    @DisplayName("Батчи топика уходят только на приемники своей задачи")
    void testTopicGoesOnlyToItsOwnTargets() {
        KafkaReplicationClient client = clientWithThreeTargets();
        client.updateTopicRoutes(Map.of(
                "orders", List.of("kafka-dc2-a:50052", "kafka-dc2-b:50052"),
                "audit", List.of("kafka-dc3-a:50052")));

        try {
            for (int partition = 0; partition < 32; partition++) {
                int ordersTarget = client.selectTargetIndex("orders", partition);
                assertTrue(ordersTarget == 0 || ordersTarget == 1,
                        "Топик 'orders' ушел на приемник чужого ЦОД (индекс " + ordersTarget + ")");

                int auditTarget = client.selectTargetIndex("audit", partition);
                assertEquals(2, auditTarget,
                        "Топик 'audit' обязан идти только на приемник своей задачи");
            }
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Партиции одного топика распределяются между приемниками его задачи")
    void testPartitionsSpreadWithinOwnTargets() {
        KafkaReplicationClient client = clientWithThreeTargets();
        client.updateTopicRoutes(Map.of(
                "orders", List.of("kafka-dc2-a:50052", "kafka-dc2-b:50052")));

        try {
            boolean sawFirst = false;
            boolean sawSecond = false;
            for (int partition = 0; partition < 64; partition++) {
                int target = client.selectTargetIndex("orders", partition);
                sawFirst |= target == 0;
                sawSecond |= target == 1;
            }
            assertTrue(sawFirst && sawSecond,
                    "Балансировка внутри задачи должна сохраниться: партиции распределяются по ее приемникам");
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Маршрутизируемый топик без своих приемников в пуле никуда не отправляется")
    void testRoutedTopicWithoutItsTargetsFailsClosed() {
        KafkaReplicationClient client = clientWithThreeTargets();
        // Приемники задачи еще не появились в пуле: маршрут указывает на узлы,
        // которых среди эндпоинтов нет
        client.updateTopicRoutes(Map.of(
                "orders", List.of("kafka-dc4-a:50052", "kafka-dc4-b:50052")));

        try {
            for (int partition = 0; partition < 8; partition++) {
                assertEquals(KafkaReplicationClient.NO_TARGET,
                        client.selectTargetIndex("orders", partition),
                        "Батч обязан остаться неотправленным: подстановка любого приемника из пула "
                                + "уводит данные маршрутизируемого топика в чужой кластер");
            }
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Появление приемников задачи в пуле возвращает маршрут в работу")
    void testRouteRecoversWhenTargetsAppear() {
        KafkaReplicationClient client = clientWithThreeTargets();
        client.updateTopicRoutes(Map.of("orders", List.of("kafka-dc4-a:50052")));
        try {
            assertEquals(KafkaReplicationClient.NO_TARGET, client.selectTargetIndex("orders", 0));

            // Пул обновился и приемник задачи появился
            client.updateEndpoints(List.of("kafka-dc4-a:50052"));

            assertEquals(0, client.selectTargetIndex("orders", 0),
                    "После появления своего приемника топик обязан снова маршрутизироваться");
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Без маршрутов используется весь пул приемников")
    void testFallsBackToWholePoolWithoutRoutes() {
        KafkaReplicationClient client = clientWithThreeTargets();

        try {
            for (int partition = 0; partition < 16; partition++) {
                int target = client.selectTargetIndex("orders", partition);
                assertTrue(target >= 0 && target <= 2,
                        "Без маршрутов поведение прежнее: выбор из всего пула");
            }
        } finally {
            client.close();
        }
    }

    @Test
    @DisplayName("Неизвестный топик не блокирует репликацию")
    void testUnknownTopicStillGetsTarget() {
        KafkaReplicationClient client = clientWithThreeTargets();
        client.updateTopicRoutes(Map.of("orders", List.of("kafka-dc2-a:50052")));

        try {
            int target = client.selectTargetIndex("unmapped-topic", 3);
            assertTrue(target >= 0 && target <= 2,
                    "Для топика без маршрута выбирается приемник из общего пула");
        } finally {
            client.close();
        }
    }
}
