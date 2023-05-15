package utils;

import exceptions.DBAppException;
import exceptions.DBSchemaException;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
                                         Hashtable<String, Hashtable<String, String>> htblColNameMetaData) throws DBAppException {
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
                                         Hashtable<String, String> htblColNameMax) throws DBAppException {
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

    public static boolean isMidValue(Object value, String min, String max) throws DBAppException {
        Comparable compValue = (Comparable) value;
        if (isString(value))
            return compValue.compareTo(min) >= 0 && ((String) value).compareTo(max) <= 0;
        if (isInteger(value))
            return compValue.compareTo(Integer.parseInt(min)) >= 0 && ((Integer) value).compareTo(Integer.parseInt(max)) <= 0;
        if (isDouble(value))
            return compValue.compareTo(Double.parseDouble(min)) >= 0 && ((Double) value).compareTo(Double.parseDouble(max)) <= 0;
        if (isDate(value))
            return compValue.compareTo(getComparable(min, "date")) >= 0 && compValue.compareTo(getComparable(max, "date")) <= 0;
        return false;
    }

    public static Comparable getComparable(String obj, String type) throws DBAppException {
        type = type.toLowerCase();
        if (type.contains("string"))
            return obj;
        if (type.contains("integer"))
            return Integer.parseInt(obj);
        if (type.contains("double"))
            return Double.parseDouble(obj);
        if (type.contains("date"))
            try {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                return format.parse(obj);
            } catch (ParseException e) {
                throw new DBSchemaException("Invalid date format");
            }
        return null;
    }

    public static Comparable getMidComparable(Comparable x, Comparable y) {
        if (x instanceof String)
            return getMidString((String) x, (String) y);
        if (x instanceof Integer)
            return ((Integer) x + (Integer) y) / 2;
        if (x instanceof Double)
            return ((Double) x + (Double) y) / 2;
        if (x instanceof Date)
            return getMidDate((Date) x, (Date) y);
        return null;
    }

    private static String getMidString(String S, String T) {
        int N = Math.min(S.length(), T.length());
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++)
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {
            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0)
                if (i + 1 <= N)
                    a1[i + 1] += 26;

            a1[i] = (int)a1[i] / 2;
        }

        String ans = "";
        for (int i = 1; i <= N; i++)
            ans += (char)(a1[i] + 97);
        return ans;

    }

    private static Date getMidDate(Date startDate, Date endDate) {
        // Calculate the middle point in milliseconds
        long timeDifference = endDate.getTime() - startDate.getTime();
        long middlePoint = startDate.getTime() + (timeDifference / 2);

        // Create the middle date
        return new Date(middlePoint);
    }


    public static Comparable increment(Comparable x) {
        if (x instanceof Integer)
            return (Integer) x + 1;
        else if (x instanceof Double)
            return (Double) x + 1;
        else if (x instanceof String)
            return (String) x + "a";
        else if (x instanceof Date) {
            Date date = (Date) x;
            return new Date(date.getTime() + 60*60*24*1000);
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
    public static void validateColumnType(Object value, String expectedType) throws DBAppException {
        if (value != null && !value.getClass().getSimpleName().equals(expectedType)) {
            throw new DBQueryException("Invalid type: Expected " + expectedType + ", found " + value.getClass().getSimpleName());
        }
    }

    public static void validateTupleMetadata(Tuple tuple, String tableName) throws DBAppException {
        Hashtable<String, Hashtable<String, String>> metadata = MetaDataManager.getMetaData(tableName);

        for (String colName : tuple.getColNames()) {
            if (!metadata.containsKey(colName)) {
                throw new DBQueryException("Column '" + colName + "' does not exist in table metadata");
            }

            Hashtable<String, String> colMetadata = metadata.get(colName);
            String expectedType = colMetadata.get("ColumnType");
            Object value = tuple.getColValue(colName);

            validateColumnType(value, expectedType);
        }
    }
    public class ColumnTypeValidator {
        public static void validateColumnTypes(Tuple tuple, String tableName) throws DBAppException {
            Hashtable<String, String> columnTypes = MetaDataManager.getMetaData(tableName);
            for (String colName : tuple.getColNames()) {
                Object value = tuple.getColValue(colName);
                String expectedType = columnTypes.get(colName);
    
                if (value != null && !value.getClass().getSimpleName().equals(expectedType)) {
                    throw new DBQueryException("Invalid type for column '" + colName + "'");
                }
            }
        }
    
        public static void validateColumnTypes(Tuple tuple, Hashtable<String, Object> htblColNameValue, String tableName) throws DBAppException {
            Hashtable<String, String> columnTypes = MetaDataManager.getMetaData(tableName);
            for (String colName : htblColNameValue.keySet()) {
                Object value = htblColNameValue.get(colName);
                String expectedType = columnTypes.get(colName);
    
                if (value != null && !value.getClass().getSimpleName().equals(expectedType)) {
                    throw new DBQueryException("Invalid type for column '" + colName + "'");
                }
            }
        }
    
        public static void validateColumnTypes(Hashtable<String, Object> htblColNameValue, String tableName) throws DBAppException {
            Hashtable<String, String> columnTypes = MetaDataManager.getMetaData(tableName);
            for (String colName : htblColNameValue.keySet()) {
                Object value = htblColNameValue.get(colName);
                String expectedType = columnTypes.get(colName);
    
                if (value != null && !value.getClass().getSimpleName().equals(expectedType)) {
                    throw new DBQueryException("Invalid type for column '" + colName + "'");
                }
            }
        }
    }
   
}
