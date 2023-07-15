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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.log4j.Logger;

public abstract class ReplaceNamespace<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = Logger.getLogger(ReplaceNamespace.class);
    public static final String OVERVIEW_DOC = "Remove the namespace from STRUCT field name"
            + "<p/>properties.resourceOrder.properties.resourceOrderItem.items.properties.resource.properties.resourceCharacteristic.items will become items"
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName()
            + "</code>) or value (<code>" + Value.class.getName() + "</code>).";

    private static final String PURPOSE = "Remove the namespace from STRUCT field name";

    private static final String NS_CONFIG = "namespace";
    private static final String RENAME_CONFIG = "rename";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(NS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "Namespace")
            .define(RENAME_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(),
                    ConfigDef.Importance.LOW, "Map of current name and target name of the element");

    private String namespace;
    private List<String> renameList;
    private Map<String, String> renames;
    private static boolean add;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        namespace = config.getString(NS_CONFIG);
        renameList = config.getList(RENAME_CONFIG);
        renames = parseMap(renameList);
    }

    private Map<String, String> parseMap(List<String> renameList2) {
        final Map<String, String> map = new HashMap<String, String>();
        for (String mapping : renameList2) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException("Invalid Mapping:" + mapping);
            }
            map.put(parts[0], parts[1]);
        }
        return map;
    }

    @Override
    public R apply(R record) {
        final Object value = operatingValue(record);
        final Schema schema = operatingSchema(record);
        if (value == null && schema == null) {
            return record;
        }
        requireSchema(schema, "updating schema metadata");
        final Schema updatedSchema = getNormalizedSchema(null, schema);
        // log.trace("Applying SetSchemaMetadata SMT. Original schema: {}, updated
        // schema: {}",
        // schema, updatedSchema);
        return newRecord(record, updatedSchema);
    }

    private ConnectSchema getNormalizedSchema(Field field, Schema schema) {
        final boolean isArray = schema.type() == Schema.Type.ARRAY;
        final boolean isMap = schema.type() == Schema.Type.MAP;
        final boolean isStruct = schema.type() == Schema.Type.STRUCT;
        String name = (field == null || !isStruct) ? schema.name()
                : namespace + "." + (renames.containsKey(field.name()) ? renames.get(field.name()) : field.name());
        final ConnectSchema updatedSchema = new ConnectSchema(
                schema.type(),
                schema.isOptional(),
                schema.defaultValue(),
                name,
                schema.version(),
                schema.doc(),
                schema.parameters(),
                isStruct ? extractFields(schema.fields()) : null,
                isMap ? schema.keySchema() : null,
                isMap || isArray ? getNormalizedSchema(field, schema.valueSchema()) : null);
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
            targetFields.add((isStruct || isArray)
                    ? new Field(field.name(), field.index(), getNormalizedSchema(field, field.schema()))
                    : field);
        }
        return targetFields;
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
    public static class Key<R extends ConnectRecord<R>> extends ReplaceNamespace<R> {
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
    public static class Value<R extends ConnectRecord<R>> extends ReplaceNamespace<R> {
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
    protected Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                if (field.schema().type().equals(Schema.Type.STRUCT)) {
                    log.info("---------Inside STRUCT--------------");
                    log.info(String.format("Field Value Schema: %s", field.schema().name()));
                    //log.info(String.format("Value: %s", origStruct.get(field)));
                    newStruct.put(field, getValForStruct(field.schema(), (Struct) origStruct.get(field)));
                } else {
                    log.info(String.format("Field Value Schema: %s", field.name()));
                    newStruct.put(field, origStruct.get(field));
                }
            }
            return newStruct;
        }
        return keyOrValue;
    }

    private Struct getValForStruct(Schema newSchema, Struct oldStruct) {
        Schema schema = oldStruct.schema();
        Struct tmpStruct = new Struct(newSchema);
        log.info("new schema:"+ newSchema);
        for (Field field : schema.fields()) {
            if (field.schema().type().equals(Schema.Type.ARRAY)) {
                log.info("---------Inside ARRAY--------------");
                log.info(String.format("Field Value Schema: %s", field.schema().valueSchema().name()));
                log.info(String.format("Field Name: %s", field.name()));
                log.info(String.format("New Schema Name: %s", newSchema));
                log.info(String.format("New Field Value Schema: %s", newSchema.field(field.name())));

                //log.info(String.format("Value: %s", oldStruct.get(field)));
                tmpStruct.put(newSchema.field(field.name()), 
                            getValForArray(newSchema.field(field.name()).schema().valueSchema(), 
                                (List) oldStruct.get(field)));
                log.info(String.format("Array completed -" + newSchema.field(field.name())));
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                log.info("---------Inside STRUCT--------------");
                log.info(String.format("Field Name: %s", field.name()));
                log.info(String.format("New Schema Name: %s", newSchema));
                log.info(String.format("Value: %s", oldStruct.get(field)));
                tmpStruct.put(newSchema.field(field.name()), getValForStruct(newSchema.field(field.name()).schema(), 
                        (Struct) oldStruct.get(field)));
            } else {
                log.info(String.format("Field Value Schema: %s", field.name()));
                tmpStruct.put(field, oldStruct.get(field));
            }
        }
        log.info(String.format("Final Struct-%s",tmpStruct), null);
        return tmpStruct;
    }

    private Object getValForArray(Schema newSchema, List<Struct> oldStructList) {
        if (oldStructList == null)
            return oldStructList;
        
        List<Struct> tmpStructList = new ArrayList<Struct>(oldStructList.size());
        for (Object object : oldStructList) {
            if (object instanceof Struct) {
                add = tmpStructList.add(getValForStruct(newSchema, (Struct) object));
                log.info("Inside getValForArray, element added:" + add);
            }
        }
        return tmpStructList;
    }
}
