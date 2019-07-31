package org.apache.flink.streaming.examples.kjtest;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


/**
 * @Title: CustomerJsonDeserialization
 * @ProjectName dw-streaming
 * @Description: TODO
 * @author Linchong
 * @date 2019/1/5 15:08
 */
public class CustomerJsonDeserialization extends AbstractDeserializationSchema<Row> {
    private static Logger log = LoggerFactory.getLogger(CustomerJsonDeserialization.class);
    /**
	 */
	private static final long serialVersionUID = 1L;

	private final ObjectMapper objectMapper = new ObjectMapper();

    private final TypeInformation<Row> typeInfo;

    private final String[] fieldNames;

    private final TypeInformation<?>[] fieldTypes;
    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    public CustomerJsonDeserialization(TypeInformation<Row> typeInfo){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();

    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            JsonNode root = objectMapper.readTree(message);
            Row row  = new Row(fieldNames.length);
            //是否需要解析json
            for (int i = 0; i < fieldNames.length; i++) {
                log.info("fieldName:{}, fieldTypes:{}",fieldNames[i],fieldTypes[i].getTypeClass());
                JsonNode node = null;
                if (fieldNames[i].contains(".rowtime")) {
                    int index = fieldNames[i].indexOf(".");
                    node = getIgnoreCase(root,fieldNames[i].substring(0,index));
                } else {
                    node = getIgnoreCase(root, fieldNames[i]);
                }
                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                    if (value != null) {
                        log.info("value.toString:{}",value.toString());
                    }
                    log.info("value:{}",value);
                    row.setField(i, value);
                }
            }
            return row;
        } catch (Throwable t) {
            throw new IOException("Failed to deserialize JSON object.", t);
        }
    }

    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

    public JsonNode getIgnoreCase(JsonNode jsonNode, String key) {

        Iterator<String> iter = jsonNode.fieldNames();
        while (iter.hasNext()) {
            String key1 = iter.next();
            if (key1.equalsIgnoreCase(key)) {
                return jsonNode.get(key1);
            }
        }

        return null;

    }
}
