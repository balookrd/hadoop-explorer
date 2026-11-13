package io.github.balookrd.pristan.iskra.service.engine.livy;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.balookrd.pristan.iskra.dto.ColumnMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Livy statement output: {"status":"ok","data":{"text/plain":..., "application/json":{"schema":..., "data":[...]}}}
 * or {"status":"error","ename":...,"evalue":...,"traceback":[...]}. Tables come from kind=sql.
 */
public final class LivyOutputParser {

    public record Parsed(List<ColumnMetadata> columns, List<List<Object>> rows, String text, String error) {}

    /** Reads Livy responses: floats as BigDecimal, so decimal(p,s) columns keep every digit. */
    static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();

    private LivyOutputParser() {
    }

    public static Parsed parse(JsonNode output) {
        if (output == null || output.isNull() || output.isMissingNode()) {
            return new Parsed(List.of(), List.of(), "", null);
        }
        if ("error".equals(output.path("status").asText())) {
            String error = output.path("ename").asText("Error") + ": " + output.path("evalue").asText("");
            StringBuilder trace = new StringBuilder();
            output.path("traceback").forEach(line -> trace.append(line.asText()));
            return new Parsed(List.of(), List.of(), trace.toString(), error);
        }
        JsonNode data = output.path("data");
        String text = data.path("text/plain").asText("");
        JsonNode table = data.path("application/json");
        if (!table.has("schema") || !table.has("data")) {
            return new Parsed(List.of(), List.of(), text, null);
        }
        List<ColumnMetadata> columns = new ArrayList<>();
        for (JsonNode field : table.path("schema").path("fields")) {
            JsonNode type = field.path("type");
            columns.add(new ColumnMetadata(field.path("name").asText(), type.isTextual() ? type.asText() : type.path("type").asText("struct")));
        }
        List<List<Object>> rows = new ArrayList<>();
        for (JsonNode row : table.path("data")) {
            List<Object> values = new ArrayList<>(row.size());
            int index = 0;
            for (JsonNode cell : row) {
                boolean decimal = index < columns.size() && columns.get(index).type().startsWith("decimal");
                values.add(decimal ? toDecimalText(cell) : toValue(cell));
                index++;
            }
            rows.add(Collections.unmodifiableList(values));
        }
        return new Parsed(columns, rows, text, null);
    }

    /**
     * decimal(p,s) as exact text: a JSON number would be read as double by the browser and lose digits
     * beyond 15-17 significant ones.
     */
    private static Object toDecimalText(JsonNode cell) {
        if (cell == null || cell.isNull()) {
            return null;
        }
        return cell.isNumber() ? cell.decimalValue().toPlainString() : cell.asText();
    }

    private static Object toValue(JsonNode cell) {
        if (cell == null || cell.isNull()) {
            return null;
        }
        if (cell.isTextual()) {
            return cell.asText();
        }
        if (cell.isBoolean()) {
            return cell.asBoolean();
        }
        if (cell.isIntegralNumber()) {
            return cell.canConvertToLong() ? cell.asLong() : cell.asText();
        }
        if (cell.isNumber()) {
            return cell.asDouble();
        }
        return cell.toString(); // arrays, maps, structs as JSON text
    }
}
