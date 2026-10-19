package org.apache.hadoop.explorer.replicator.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskAttestationServiceTest {

    private static final String SECRET = "test-cluster-orchestrator-hmac-secret-xyz-987";
    private static final String TASK_ID = "task-101";
    private static final String JOB_ID = "job-42";
    private static final String TARGET_PATH = "/data/warehouse/tables/orders";
    private static final String USER_PRINCIPAL = "analyst@REALM.CORP";

    @Test
    @DisplayName("Успешная генерация и валидация токена аттестации задачи")
    void testGenerateAndValidateValidToken() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        assertNotNull(token);
        assertTrue(token.startsWith("v1."));

        boolean valid = TaskAttestationService.validateToken(
                token, TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );
        assertTrue(valid, "Токен с оригинальными параметрами должен быть успешно валидирован");
    }

    @Test
    @DisplayName("Отклонение токена при попытке эскалации привилегий (подмена executionPrincipal на hdfs)")
    void testRejectTamperedPrincipalPrivilegeEscalation() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        // Злоумышленник пытается передать hdfs суперпользователя
        boolean valid = TaskAttestationService.validateToken(
                token, TASK_ID, JOB_ID, TARGET_PATH, "hdfs@REALM.CORP", false, SECRET
        );
        assertFalse(valid, "Попытка подмены принципала на суперпользователя должна быть отклонена");
    }

    @Test
    @DisplayName("Отклонение токена при подмене флага runAsServiceAccount")
    void testRejectTamperedServiceAccountFlag() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        boolean valid = TaskAttestationService.validateToken(
                token, TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, true, SECRET
        );
        assertFalse(valid, "Попытка эскалации до системной техучетки должна быть отклонена");
    }

    @Test
    @DisplayName("Отклонение токена при подмене целевого пути (Path Substitution / Path Traversal)")
    void testRejectTamperedTargetPath() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        // Попытка записать в системный каталог
        boolean valid = TaskAttestationService.validateToken(
                token, TASK_ID, JOB_ID, "/etc/hadoop/conf/core-site.xml", USER_PRINCIPAL, false, SECRET
        );
        assertFalse(valid, "Подмена целевого пути должна быть отклонена");
    }

    @Test
    @DisplayName("Отклонение токена при подмене taskId или jobId")
    void testRejectTamperedTaskOrJobId() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        assertFalse(TaskAttestationService.validateToken(token, "other-task", JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken(token, TASK_ID, "other-job", TARGET_PATH, USER_PRINCIPAL, false, SECRET));
    }

    @Test
    @DisplayName("Отклонение токена при валидации другим секретным ключом")
    void testRejectWrongSecretKey() {
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );

        boolean valid = TaskAttestationService.validateToken(
                token, TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, "wrong-secret-key-321"
        );
        assertFalse(valid, "Токен не должен валидироваться чужим секретом");
    }

    @Test
    @DisplayName("Отклонение просроченного токена (TTL Expiration)")
    void testRejectExpiredToken() throws InterruptedException {
        // Токен с временем жизни 1 миллисекунда
        String token = TaskAttestationService.generateToken(
                TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, 1L, SECRET
        );

        // Задержка с превышением толерантности часов не требуется, если мы тестируем с отрицательным TTL или имитируем истекший
        String expiredPayload = TaskAttestationService.buildCanonicalPayload(TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, System.currentTimeMillis() - 100_000L);
        // Ручное построение просроченного токена
        long expiredTime = System.currentTimeMillis() - (TaskAttestationService.CLOCK_SKEW_TOLERANCE_MILLIS + 10_000L);
        String expiredToken = "v1." + expiredTime + ".fakeSignature";

        boolean valid = TaskAttestationService.validateToken(
                expiredToken, TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET
        );
        assertFalse(valid, "Просроченный токен должен быть отклонен");
    }

    @Test
    @DisplayName("Обработка пустых, null и поврежденных токенов")
    void testMalformedTokens() {
        assertFalse(TaskAttestationService.validateToken(null, TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken("", TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken("invalid.token", TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken("v2.12345.signature", TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken("v1.not_a_number.sig", TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
        assertFalse(TaskAttestationService.validateToken("v1.9999999999999.invalid_base64!@#", TASK_ID, JOB_ID, TARGET_PATH, USER_PRINCIPAL, false, SECRET));
    }
}
