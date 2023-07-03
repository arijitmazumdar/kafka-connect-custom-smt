/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.connect.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RemoveSubNamespace<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(RemoveSubNamespace.class);
    public static final String OVERVIEW_DOC = "Remove the namespace from STRUCT field name"
            + "<p/>properties.resourceOrder.properties.resourceOrderItem.items.properties.resource.properties.resourceCharacteristic.items will become items"
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
            + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static final String PURPOSE = "Remove the namespace from STRUCT field name";

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    }

    @Override
    public R apply(R record) {
        final Object value = operatingValue(record);
        final Schema schema = operatingSchema(record);
        if (value == null && schema == null) {
            return record;
        }
        requireSchema(schema, "updating schema metadata");
        final Schema updatedSchema = getNormalizedSchema(schema);
        log.trace("Applying SetSchemaMetadata SMT. Original schema: {}, updated schema: {}",
                schema, updatedSchema);
        return newRecord(record, updatedSchema);
    }

    private Schema getNormalizedSchema(Schema schema) {
        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        final boolean isStruct = schema.type() == Schema.Type.STRUCT;
        final Schema updatedSchema = new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                isStruct ? removeNameSpaceFromSchemaName(schema.name()) : schema.name(),
                schema.version(),
                schema.doc(),
                schema.parameters(),
                isStruct ? extractFields(schema.fields()): null,
                isMap ? schema.keySchema() : null,
                isMap || isArray ? getNormalizedSchema(schema.valueSchema()) : null);       
        return updatedSchema;
    }

    private List<Field> extractFields(List<Field> fields) {
        if (fields == null)
            return fields;
        List<Field> targetFields = new ArrayList<Field>();
        
        for (Field field : fields) {

            final boolean isArray = field.schema().type() == Schema.Type.ARRAY;
            final boolean isMap = field.schema().type() == Schema.Type.MAP;
            final boolean isStruct = field.schema().type() == Schema.Type.STRUCT;
            if (isStruct || isArray) {
                final ConnectSchema s = new ConnectSchema(
                field.schema().type(),
                field.schema().isOptional(),
                field.schema().defaultValue(),
                field.name(),
                field.schema().version(),
                field.schema().doc(),
                field.schema().parameters(),
                extractFields(field.schema().fields()),
                isMap ? field.schema().keySchema() : null,
                isMap || isArray ? getNormalizedSchema(field.schema().valueSchema()) : null);

                log.info("extractFields::Converted from:" + field.schema().name() + "to:"+ field.name());
                Field nField = new Field(field.name(),
                                        field.index(),
                                        s);
                targetFields.add(nField);
            } else {
                targetFields.add(field);
            }
        }
        return targetFields;
    }

    private static String removeNameSpaceFromSchemaName(String schemaName) {
        if(schemaName == null || !schemaName.contains(".")) {
            return schemaName;
        }
        //from: properties.resourceOrder.properties.resourceOrderItem.items.properties.resource.properties.resourceCharacteristic.items
        //to: resourceCharacteristic
        String[] splittedArr = schemaName.split("\\.");
        log.info("removeNameSpaceFromSchemaName:" + splittedArr[splittedArr.length - 2]);
        return splittedArr[splittedArr.length - 2];
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema);

    /**
     * Set the schema name, version or both on the record's key schema.
     */
    public static class Key<R extends ConnectRecord<R>> extends RemoveSubNamespace<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedKey = updateSchemaIn(record.key(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedKey,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Set the schema name, version or both on the record's value schema.
     */
    public static class Value<R extends ConnectRecord<R>> extends RemoveSubNamespace<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema) {
            Object updatedValue = updateSchemaIn(record.value(), updatedSchema);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }
    }

    /**
     * Utility to check the supplied key or value for references to the old Schema,
     * and if so to return an updated key or value object that references the new
     * Schema.
     * Note that this method assumes that the new Schema may have a different name
     * and/or version,
     * but has fields that exactly match those of the old Schema.
     * <p>
     * Currently only {@link Struct} objects have references to the {@link Schema}.
     *
     * @param keyOrValue    the key or value object; may be null
     * @param updatedSchema the updated schema that has been potentially renamed
     * @return the original key or value object if it does not reference the old
     *         schema, or
     *         a copy of the key or value object with updated references to the new
     *         schema.
     */
    protected static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                // assume both schemas have exact same fields with same names and schemas ...
                log.info("key:" + field + ",value:" + origStruct.get(field));
                if(!(field.schema().type().equals(Schema.Type.STRUCT) ||
                    field.schema().type().equals(Schema.Type.ARRAY))) {
                    //log.info("In field:" + field.schema().name());
                    newStruct.put(field, origStruct.get(field));
                } 
            }
            return newStruct;
        }
        return keyOrValue;
    }
}
