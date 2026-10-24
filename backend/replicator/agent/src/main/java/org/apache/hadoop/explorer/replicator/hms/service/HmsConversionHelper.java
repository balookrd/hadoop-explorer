package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.generated.HmsFieldSchemaProto;
import org.apache.hadoop.explorer.replicator.generated.HmsNotificationEventProto;
import org.apache.hadoop.explorer.replicator.generated.HmsPartitionProto;
import org.apache.hadoop.explorer.replicator.generated.HmsTableProto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HmsConversionHelper {

    private HmsConversionHelper() {}

    public static HmsFieldSchemaProto toProto(HmsFieldSchemaDto dto) {
        if (dto == null) return HmsFieldSchemaProto.getDefaultInstance();
        return HmsFieldSchemaProto.newBuilder()
                .setName(dto.name() != null ? dto.name() : "")
                .setType(dto.type() != null ? dto.type() : "string")
                .setComment(dto.comment() != null ? dto.comment() : "")
                .build();
    }

    public static HmsFieldSchemaDto toDto(HmsFieldSchemaProto proto) {
        if (proto == null) return null;
        return new HmsFieldSchemaDto(
                proto.getName(),
                proto.getType(),
                proto.getComment()
        );
    }

    public static HmsTableProto toProto(HmsTableDto dto) {
        if (dto == null) return HmsTableProto.getDefaultInstance();
        HmsTableProto.Builder b = HmsTableProto.newBuilder()
                .setCatalogName(dto.catName() != null ? dto.catName() : "hive")
                .setDbName(dto.dbName() != null ? dto.dbName() : "")
                .setTableName(dto.tableName() != null ? dto.tableName() : "")
                .setTableType(dto.tableType() != null ? dto.tableType() : "")
                .setSdLocation(dto.sdLocation() != null ? dto.sdLocation() : "")
                .setInputFormat(dto.inputFormat() != null ? dto.inputFormat() : "")
                .setOutputFormat(dto.outputFormat() != null ? dto.outputFormat() : "")
                .setSerdeLib(dto.serdeLib() != null ? dto.serdeLib() : "");

        if (dto.parameters() != null) {
            b.putAllParameters(dto.parameters());
        }
        if (dto.partitionKeys() != null) {
            b.addAllPartitionKeys(dto.partitionKeys());
        }
        if (dto.columns() != null) {
            for (HmsFieldSchemaDto col : dto.columns()) {
                b.addColumns(toProto(col));
            }
        }
        if (dto.partitionKeyFields() != null) {
            for (HmsFieldSchemaDto pk : dto.partitionKeyFields()) {
                b.addPartitionKeyFields(toProto(pk));
            }
        }
        if (dto.serdeParameters() != null) {
            b.putAllSerdeParameters(dto.serdeParameters());
        }
        return b.build();
    }

    public static HmsTableDto toDto(HmsTableProto proto) {
        if (proto == null) return null;

        List<HmsFieldSchemaDto> cols = new ArrayList<>();
        if (proto.getColumnsList() != null) {
            for (HmsFieldSchemaProto cp : proto.getColumnsList()) {
                cols.add(toDto(cp));
            }
        }

        List<HmsFieldSchemaDto> pkFields = new ArrayList<>();
        if (proto.getPartitionKeyFieldsList() != null && !proto.getPartitionKeyFieldsList().isEmpty()) {
            for (HmsFieldSchemaProto pkp : proto.getPartitionKeyFieldsList()) {
                pkFields.add(toDto(pkp));
            }
        } else if (proto.getPartitionKeysList() != null) {
            for (String pk : proto.getPartitionKeysList()) {
                pkFields.add(new HmsFieldSchemaDto(pk, "string", null));
            }
        }

        return new HmsTableDto(
                proto.getCatalogName(),
                proto.getDbName(),
                proto.getTableName(),
                proto.getTableType(),
                proto.getSdLocation(),
                new HashMap<>(proto.getParametersMap()),
                new ArrayList<>(proto.getPartitionKeysList()),
                proto.getInputFormat(),
                proto.getOutputFormat(),
                proto.getSerdeLib(),
                cols,
                pkFields,
                new HashMap<>(proto.getSerdeParametersMap())
        );
    }

    public static HmsPartitionProto toProto(HmsPartitionDto dto) {
        if (dto == null) return HmsPartitionProto.getDefaultInstance();
        HmsPartitionProto.Builder b = HmsPartitionProto.newBuilder()
                .setCatalogName(dto.catName() != null ? dto.catName() : "hive")
                .setDbName(dto.dbName() != null ? dto.dbName() : "")
                .setTableName(dto.tableName() != null ? dto.tableName() : "")
                .setLocation(dto.location() != null ? dto.location() : "");

        if (dto.values() != null) {
            b.addAllValues(dto.values());
        }
        if (dto.parameters() != null) {
            b.putAllParameters(dto.parameters());
        }
        if (dto.columns() != null) {
            for (HmsFieldSchemaDto col : dto.columns()) {
                b.addColumns(toProto(col));
            }
        }
        if (dto.serdeParameters() != null) {
            b.putAllSerdeParameters(dto.serdeParameters());
        }
        if (dto.inputFormat() != null) {
            b.setInputFormat(dto.inputFormat());
        }
        if (dto.outputFormat() != null) {
            b.setOutputFormat(dto.outputFormat());
        }
        if (dto.serdeLib() != null) {
            b.setSerdeLib(dto.serdeLib());
        }
        return b.build();
    }

    public static HmsPartitionDto toDto(HmsPartitionProto proto) {
        if (proto == null) return null;
        List<HmsFieldSchemaDto> cols = new ArrayList<>();
        if (proto.getColumnsList() != null) {
            for (HmsFieldSchemaProto cp : proto.getColumnsList()) {
                cols.add(toDto(cp));
            }
        }
        return new HmsPartitionDto(
                proto.getCatalogName(),
                proto.getDbName(),
                proto.getTableName(),
                new ArrayList<>(proto.getValuesList()),
                proto.getLocation(),
                new HashMap<>(proto.getParametersMap()),
                cols,
                new HashMap<>(proto.getSerdeParametersMap()),
                emptyToNull(proto.getInputFormat()),
                emptyToNull(proto.getOutputFormat()),
                emptyToNull(proto.getSerdeLib())
        );
    }

    /** Protobuf не различает «не задано» и пустую строку — приводим к null для DTO. */
    private static String emptyToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }

    public static HmsNotificationEventProto toProto(HmsNotificationEventDto dto) {
        if (dto == null) return HmsNotificationEventProto.getDefaultInstance();
        return HmsNotificationEventProto.newBuilder()
                .setEventId(dto.eventId())
                .setEventTime(dto.eventTime())
                .setEventType(dto.eventType() != null ? dto.eventType() : "")
                .setDbName(dto.dbName() != null ? dto.dbName() : "")
                .setTableName(dto.tableName() != null ? dto.tableName() : "")
                .setMessage(dto.message() != null ? dto.message() : "")
                .setMessageFormat(dto.messageFormat() != null ? dto.messageFormat() : "JSON")
                .build();
    }

    public static HmsNotificationEventDto toDto(HmsNotificationEventProto proto) {
        if (proto == null) return null;
        return new HmsNotificationEventDto(
                proto.getEventId(),
                proto.getEventTime(),
                proto.getEventType(),
                proto.getDbName(),
                proto.getTableName(),
                proto.getMessage(),
                proto.getMessageFormat()
        );
    }
}
