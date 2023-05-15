package utils;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

public class Validation {

    public static boolean isTableExists(String strTableName) {
        String tableFile = Utils.getTableFilePath(strTableName);
        return new File(tableFile).exists();
    }

    public static boolean areAllowedDataTypes(Hashtable<String, String> htblColNameType) {
        String[] arrAllowedDataTypes = {"java.lang.string", "java.lang.integer", "java.lang.double", "java.util.date"};
        List<String> allowedDataTypes = Arrays.asList(arrAllowedDataTypes);

        for (String ColType : htblColNameType.values())
            if (!allowedDataTypes.contains(ColType.toLowerCase()))
                return false;
        return true;
    }

    public static boolean validateSchema(Hashtable<String, Object> htblColNameValue,
                                         Hashtable<String, Hashtable<String, String>> htblColNameMetaData) throws ParseException {
        for (String colName : htblColNameValue.keySet()) {
            Hashtable<String, String> colMetaData = htblColNameMetaData.get(colName);
            String type = colMetaData.get("ColumnType");
            Object value = htblColNameValue.get(colName);
            if (!isNeededType(value, type) || !isMidValue(value, colMetaData.get("Min"), colMetaData.get("Max")))
                return false;
        }
        return true;
    }

    public static boolean validateMinMax(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                                         Hashtable<String, String> htblColNameMax) throws ParseException {
        for (String colName : htblColNameType.keySet()) {
            String type = htblColNameType.get(colName);
            String min = htblColNameMin.get(colName);
            String max = htblColNameMax.get(colName);
            if (!isNeededType(min, type) || !isNeededType(max, type)) // Validate min and max schema
                return false;

            Comparable compMin = getComparable(min, type);
            Comparable compMax = getComparable(max, type);
            if (compMin.compareTo(compMax) > 0) // Validate min <= max always
                return false;
        }
        return true;
    }


    // Case-insensitive
    public static boolean isNeededType(Object obj, String type) {
        type = type.toLowerCase();
        if (isString(obj) && type.contains("string"))
            return true;
        if (isInteger(obj) && type.contains("integer"))
            return true;
        if (isDouble(obj) && type.contains("double"))
            return true;
        if (isDate(obj) && type.contains("date"))
            return true;

        return false;
    }

    public static boolean isNeededType(String obj, String type) {
        type = type.toLowerCase();
        if (isString(obj) && type.contains("string"))
            return true;
        if (isInteger(obj) && type.contains("integer"))
            return true;
        if (isDouble(obj) && type.contains("double"))
            return true;
        if (isDate(obj) && type.contains("date"))
            return true;

        return false;
    }

    public static boolean isMidValue(Object value, String min, String max) throws ParseException {
        Comparable compValue = (Comparable) value;
        if (isString(value))
            return compValue.compareTo(min) >= 0 && ((String) value).compareTo(max) <= 0;
        if (isInteger(value))
            return compValue.compareTo(Integer.parseInt(min)) >= 0 && ((Integer) value).compareTo(Integer.parseInt(max)) <= 0;
        if (isDouble(value))
            return compValue.compareTo(Double.parseDouble(min)) >= 0 && ((Double) value).compareTo(Double.parseDouble(max)) <= 0;
        if (isDate(value)) {
            try {
                return compValue.compareTo(getComparable(min, "date")) >= 0 && compValue.compareTo(getComparable(max, "date")) <= 0;
            } catch (ParseException e) {
            }
        }
        return false;
    }

    public static Comparable getComparable(String obj, String type) throws ParseException {
        type = type.toLowerCase();
        if (type.contains("string"))
            return obj;
        if (type.contains("integer"))
            return Integer.parseInt(obj);
        if (type.contains("double"))
            return Double.parseDouble(obj);
        if (type.contains("date")) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            return format.parse(obj);
        }
        return null;
    }


    private static boolean isString(Object obj) {
        return obj instanceof String;
    }

    private static boolean isInteger(Object obj) {
        return obj instanceof Integer;
    }

    private static boolean isDouble(Object obj) {
        return obj instanceof Double;
    }

    private static boolean isDate(Object obj) {
        return obj instanceof java.util.Date;
    }


    private static boolean isString(String str) {
        return str != null && str.length() > 0;
    }

    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private static boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private static boolean isDate(String str) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            format.parse(str);
        } catch (Exception e) {
            return false;
        }
        return true;
    }


}
