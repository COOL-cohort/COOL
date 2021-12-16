package com.nus.cool.core.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Schema for data
 */
public class TableSchema {

    @Getter
    @Setter
    private String charset;

    @Getter
    private List<FieldSchema> fields;

    private Map<String, Integer> name2Id = Maps.newHashMap();

    /**
     * UserKeyField index, assign -1 if this field
     * type not exist.
     */
    @Getter
    private int userKeyField = -1;

    /**
     * AppKeyField index, assign -1 if this field
     * type not exist.
     */
    @Getter
    private int appKeyField = -1;

    /**
     * ActionField index, assign -1 if this field
     * type not exist.
     */
    @Getter
    private int actionField = -1;

    /**
     * ActionTimeField index, assign -1 if this field
     * type not exist.
     */
    @Getter
    private int actionTimeField = -1;

    public static TableSchema read(InputStream in) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(in, TableSchema.class);
    }

    public void setFields(List<FieldSchema> fields) {
        this.fields = fields;
        this.name2Id.clear();
        for (int i = 0; i < fields.size(); i++) {
            FieldSchema field = fields.get(i);
            FieldType fieldType = field.getFieldType();
            this.name2Id.put(field.getName(), i);
            switch (fieldType) {
                case AppKey:
                    this.appKeyField = i;
                    break;
                case UserKey:
                    this.userKeyField = i;
                    break;
                case Action:
                    this.actionField = i;
                    break;
                case ActionTime:
                    this.actionTimeField = i;
                    break;
                default:
                    break;
            }
        }
    }

    public FieldSchema getField(int id) {
        return this.fields.get(id);
    }

    public FieldSchema getField(String name) {
        return this.getField(this.getFieldID(name));
    }

    public FieldType getFieldType(String name) {
        return this.getField(name).getFieldType();
    }

    public int getFieldID(String name) {
        Integer id = this.name2Id.get(name);
        return id == null ? -1 : id;
    }

    public String getActionTimeFieldName() {
        return this.fields.get(this.getActionTimeField()).getName();
    }
}
