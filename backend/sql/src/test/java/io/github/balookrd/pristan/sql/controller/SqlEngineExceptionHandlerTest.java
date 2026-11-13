package io.github.balookrd.pristan.sql.controller;

import io.github.balookrd.pristan.sql.service.engine.SqlEngineException;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlEngineExceptionHandlerTest {

    @Test
    void engineFailureIsBadGatewayWithDetail() {
        ResponseEntity<Map<String, Object>> response = new SqlEngineExceptionHandler()
                .handle(new SqlEngineException("Trino Production: Connection refused"));

        assertEquals(502, response.getStatusCode().value());
        assertEquals("Trino Production: Connection refused", response.getBody().get("detail"));
        assertEquals(502, response.getBody().get("status"));
    }

    @Test
    void nullMessageStillProducesBody() {
        ResponseEntity<Map<String, Object>> response = new SqlEngineExceptionHandler().handle(new SqlEngineException(null));

        assertEquals(502, response.getStatusCode().value());
        assertEquals("Ошибка SQL-движка", response.getBody().get("detail"));
    }
}
