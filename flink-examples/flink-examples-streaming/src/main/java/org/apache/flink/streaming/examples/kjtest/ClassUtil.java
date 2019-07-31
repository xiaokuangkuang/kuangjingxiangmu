package org.apache.flink.streaming.examples.kjtest;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title: ClassUtil
 * @ProjectName dw-streaming
 * @Description: TODO
 * @author Linchong
 * @date 2019/1/7 17:40
 */
public class ClassUtil {

    public static Class<?> str2Class(String type) {
        switch (type.toLowerCase()) {
            case "boolean":
            case "bool":
                return Boolean.class;
            case "int":
            case "integer":
                return Integer.class;
            case "bigint":
            case "long":
                return Long.class;
            case "tinyint":
                return Byte.class;
            case "smallint":
                return Short.class;
            case "varchar":
            case "string":
                return String.class;
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "date":
                return Date.class;
            case "timestamp":
                return Timestamp.class;
            case "decimal":
            case "bigdecimal":
                return BigDecimal.class;
        }
        throw new RuntimeException("不支持[" + type + "]类型");
    }

    /**
     * 字符串根据字段类型转换成相应的字段值
     * @param type
     * @param val
     * @return
     */
    public static Object str2Object(String type, String val) {
        switch (type.toLowerCase().trim()) {
            case "boolean":
                return Boolean.parseBoolean(val);
            case "int":
                return Integer.parseInt(val);
            case "bigint":
                return Long.parseLong(val);
            case "long":
                return Long.parseLong(val);
            case "tinyint":
                return Byte.parseByte(val);
            case "smallint":
                return Short.parseShort(val);
            case "varchar":
            case "string":
                return val;
            case "float":
                return Float.parseFloat(val);
            case "double":
                return Double.parseDouble(val);
            case "date":
                return Date.valueOf(val);
            case "timestamp":
                return new Timestamp(Long.parseLong(val));
            case "bigdecimal":
                return new BigDecimal(val);
            case "decimal":
                return new BigDecimal(val);
            case "integer":
                return Integer.class;
        }
        throw new RuntimeException("不支持[" + type + "]类型");
    }
    
    public static boolean ifNumber(String type){
    	String mysqlType = toMysqlType(type);
    	if(mysqlNumberTypeList.contains(mysqlType.toUpperCase())){
    		return true;
    	}
    	return false;
    }
    
    
    public static List<String> mysqlNumberTypeList =  new ArrayList<String>();
    
    static{
    	mysqlNumberTypeList.add("TINYINT");
    	mysqlNumberTypeList.add("INT");
    	mysqlNumberTypeList.add("BIGINT");
    	mysqlNumberTypeList.add("FLOAT");
    	mysqlNumberTypeList.add("DOUBLE");
    	mysqlNumberTypeList.add("DECIMAL");
    }
    
    public static String toMysqlType(String type){
    	switch (type.toLowerCase().trim()) {
        case "boolean":
        case "bool":
        case "tinyint":
            return "TINYINT";
        case "int":
        case "integer":
            return "INT";
        case "bigint":
        case "long":
            return "BIGINT";
        case "datetime":
        case "timestamp":
            return "DATETIME";
        case "date":
            return "DATE";
        case "string":
            return "VARCHAR(1024)";
        case "float":
            return "FLOAT";
        case "double":
            return "DOUBLE";
        case "bigdecimal":
        case "decimal":
            return "DECIMAL";
           
    	}
    	throw new RuntimeException("不支持[" + type + "]类型");
    }


    public static String toHiveType(String type){
        switch (type.toLowerCase().trim()) {
            case "long":
                return "bigint";
                default:return type.toLowerCase().trim();
        }
    }
}
