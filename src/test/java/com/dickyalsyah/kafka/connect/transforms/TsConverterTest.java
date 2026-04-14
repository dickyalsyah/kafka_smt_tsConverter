package com.dickyalsyah.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TsConverterTest {

    private TsConverter<SourceRecord> xform;

    @BeforeEach
    public void setUp() {
        xform = new TsConverter.Value<>();
    }

    @AfterEach
    public void tearDown() {
        xform.close();
    }

    @Test
    public void testMicrosecondPrecisionPreserved() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "microseconds");

        xform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("UPDATED_AT", Schema.INT64_SCHEMA)
                .build();

        long microEpoch = 1775741713958956L;

        Struct value = new Struct(schema);
        value.put("UPDATED_AT", microEpoch);

        SourceRecord record = new SourceRecord(null, null, "test", 0, schema, value);
        SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        Object transformedTimestamp = transformedValue.get("UPDATED_AT");

        assertTrue(transformedTimestamp instanceof java.util.Date, "Should be java.util.Date (or subclass)");
        assertTrue(transformedTimestamp instanceof java.sql.Timestamp,
                "Should specifically be java.sql.Timestamp to preserve nanos");

        java.sql.Timestamp sqlTs = (java.sql.Timestamp) transformedTimestamp;
        // The nanos part of java.sql.Timestamp should contain the milliseconds AND
        // microseconds
        // epochMillis = 1775741713958
        // existingMillisNanos = (1775741713958 % 1000) * 1000000 = 958 * 1000000 =
        // 958000000
        // remainder micros = 956
        // nanos = 958000000 + 956000 = 958956000
        assertEquals(958956000, sqlTs.getNanos(), "Nanos should preserve microsecond precision");
    }

    @Test
    public void testMultipleFieldsConverted() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT,CREATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "microseconds");

        xform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("UPDATED_AT", Schema.INT64_SCHEMA)
                .field("CREATED_AT", Schema.INT64_SCHEMA)
                .field("OTHER_FIELD", Schema.STRING_SCHEMA)
                .build();

        long microEpoch1 = 1775741713958000L;
        long microEpoch2 = 1775741714000123L;

        Struct value = new Struct(schema);
        value.put("UPDATED_AT", microEpoch1);
        value.put("CREATED_AT", microEpoch2);
        value.put("OTHER_FIELD", "ignored");

        SourceRecord record = new SourceRecord(null, null, "test", 0, schema, value);
        SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct) transformed.value();

        Object ts1 = transformedValue.get("UPDATED_AT");
        Object ts2 = transformedValue.get("CREATED_AT");

        assertTrue(ts1 instanceof java.sql.Timestamp);
        assertTrue(ts2 instanceof java.sql.Timestamp);

        java.sql.Timestamp sqlTs1 = (java.sql.Timestamp) ts1;
        java.sql.Timestamp sqlTs2 = (java.sql.Timestamp) ts2;

        assertEquals(958000000, sqlTs1.getNanos());
        assertEquals(123000, sqlTs2.getNanos(), "Should preserve 123 microseconds as nanos");
        assertEquals("ignored", transformedValue.get("OTHER_FIELD"));

        assertEquals(Timestamp.SCHEMA, transformedValue.schema().field("UPDATED_AT").schema());
        assertEquals(Timestamp.SCHEMA, transformedValue.schema().field("CREATED_AT").schema());
        assertEquals(Schema.STRING_SCHEMA, transformedValue.schema().field("OTHER_FIELD").schema());
    }

    @Test
    public void testNullFieldPassthrough() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "microseconds");

        xform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("UPDATED_AT", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct value = new Struct(schema);
        value.put("UPDATED_AT", null);

        SourceRecord record = new SourceRecord(null, null, "test", 0, schema, value);
        SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        assertNull(transformedValue.get("UPDATED_AT"));
    }

    @Test
    public void testPerFieldPrecision() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT,CREATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "UPDATED_AT:milliseconds,CREATED_AT:microseconds");

        xform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("UPDATED_AT", Schema.INT64_SCHEMA)
                .field("CREATED_AT", Schema.INT64_SCHEMA)
                .build();

        long baseEpoch = 1712664000000L; // 2024-04-09 12:00:00 UTC in millis
        long microEpoch = 1712664000000000L; // same time in micros

        Struct value = new Struct(schema);
        value.put("UPDATED_AT", baseEpoch); // Millis precision
        value.put("CREATED_AT", microEpoch); // Micros precision

        SourceRecord record = new SourceRecord(null, null, "test", 0, schema, value);
        SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        java.util.Date ts1 = (java.util.Date) transformedValue.get("UPDATED_AT");
        java.util.Date ts2 = (java.util.Date) transformedValue.get("CREATED_AT");

        assertEquals(baseEpoch, ts1.getTime(), "UPDATED_AT should correctly parse from milliseconds");
        assertEquals(baseEpoch, ts2.getTime(), "CREATED_AT should correctly parse from microseconds");
    }

    @Test
    public void testSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT,CREATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "microseconds");

        xform.configure(config);

        Map<String, Object> value = new HashMap<>();
        value.put("UPDATED_AT", 1775741713958956L);
        value.put("CREATED_AT", 1775741714000123L);
        value.put("OTHER_FIELD", "ignored");

        SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        SourceRecord transformed = xform.apply(record);

        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();

        Object ts1 = transformedValue.get("UPDATED_AT");
        Object ts2 = transformedValue.get("CREATED_AT");

        assertTrue(ts1 instanceof java.sql.Timestamp);
        assertTrue(ts2 instanceof java.sql.Timestamp);
        assertEquals("ignored", transformedValue.get("OTHER_FIELD"));
    }

    @Test
    public void testTimezoneShift() {
        Map<String, String> config = new HashMap<>();
        config.put(TsConverter.FIELD_CONFIG, "UPDATED_AT,CREATED_AT");
        config.put(TsConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TsConverter.UNIX_PRECISION_CONFIG, "milliseconds");
        // Shift UPDATED_AT by +7 hours (Asia/Bangkok), let CREATED_AT fallback to
        // default (0 shift)
        config.put(TsConverter.TARGET_TIMEZONE_CONFIG, "UPDATED_AT:Asia/Bangkok");

        xform.configure(config);

        Schema schema = SchemaBuilder.struct()
                .field("UPDATED_AT", Schema.INT64_SCHEMA)
                .field("CREATED_AT", Schema.INT64_SCHEMA)
                .build();

        long baseEpoch = 1712664000000L; // 2024-04-09 12:00:00 UTC

        Struct value = new Struct(schema);
        value.put("UPDATED_AT", baseEpoch); // Target +7
        value.put("CREATED_AT", baseEpoch); // Target +0

        SourceRecord record = new SourceRecord(null, null, "test", 0, schema, value);
        SourceRecord transformed = xform.apply(record);

        Struct transformedValue = (Struct) transformed.value();
        java.util.Date ts1 = (java.util.Date) transformedValue.get("UPDATED_AT");
        java.util.Date ts2 = (java.util.Date) transformedValue.get("CREATED_AT");

        long expectedShiftedEpoch = baseEpoch + (7 * 3600 * 1000L); // 2024-04-09 19:00:00

        assertEquals(expectedShiftedEpoch, ts1.getTime(), "UPDATED_AT should be shifted +7 hours");
        assertEquals(baseEpoch, ts2.getTime(), "CREATED_AT should remain in UTC");
    }
}
