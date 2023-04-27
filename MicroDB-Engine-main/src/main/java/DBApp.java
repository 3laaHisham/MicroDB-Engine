// errors appear


import exceptions.DBAlreadyExistsException;
import exceptions.DBAppException;
import exceptions.DBNotFoundException;
import exceptions.DBSchemaException;
import model.SQLTerm;
import model.Table;
import model.Tuple;
import utils.MetaDataManager;
import utils.SerializationManager;
import utils.Validation;

import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
    private MetaDataManager metaDataManager;
    private SerializationManager serializationManager;

    public static void main(String[] args) throws Exception {

        String strTableName = "Student";
        String strClusteringKeyColumn = "id";

        DBApp dbApp = new DBApp();
        dbApp.init();

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");
        Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        htblColNameMax.put("id", "1000000000");
        htblColNameMax.put("name", "Z");
        htblColNameMax.put("gpa", "4.0");


        Hashtable htblColNameValue = new Hashtable<String, Object>();
        htblColNameValue.put("id", 2343432);
        htblColNameValue.put("name", "Alaa");
        htblColNameValue.put("gpa", 0.95);

        try {
            dbApp.createTable(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            dbApp.insertIntoTable(strTableName, htblColNameValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() throws IOException {
        metaDataManager = new MetaDataManager();
        serializationManager = new SerializationManager();
    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException, IOException, ParseException {
        if (Validation.isTableExists(strTableName))
            throw new DBAlreadyExistsException("Table already exists");
        if (!htblColNameType.containsKey(strClusteringKeyColumn))
            throw new DBSchemaException("Clustering key does not exist");
        if (!htblColNameType.keySet().equals(htblColNameMin.keySet()) || !htblColNameType.keySet().equals(htblColNameMax.keySet()))
            throw new DBSchemaException("Some columns have missing metadata");
        if (!Validation.areAllowedDataTypes(htblColNameType))
            throw new DBSchemaException("Invalid data type");
        if (!Validation.validateMinMax(htblColNameType, htblColNameMin, htblColNameMax))
            throw new DBSchemaException("min, max type do not match schema OR min > max");

        metaDataManager.createTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);

        Table table = new Table(strTableName, strClusteringKeyColumn);

        serializationManager.serializeTable(table);
    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ParseException {

        if (!Validation.isTableExists(strTableName))
            throw new DBNotFoundException("Table do not exist");
        Hashtable<String, Hashtable<String, String>> htblColNameMetaData = metaDataManager.getMetaData(strTableName);
        if (!htblColNameValue.keySet().equals(htblColNameMetaData.keySet()))
            throw new DBSchemaException("Column names do not match table schema");
        if (!Validation.validateSchema(htblColNameValue, htblColNameMetaData))
            throw new DBSchemaException("Columns metadata do not match table schema");

        Table table = serializationManager.deserializeTable(strTableName);

        String clusterKeyName = table.getClusterKeyName();
        Tuple tuple = new Tuple(clusterKeyName, htblColNameValue);

        table.insertTuple(tuple);

        serializationManager.serializeTable(table);
    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        // Todo: Cast strClusteringKeyValue to the correct type based on metadata


    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue entries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, ParseException, IOException {

        if (!Validation.isTableExists(strTableName))
            throw new DBNotFoundException("Table do not exist");
        Hashtable<String, Hashtable<String, String>> htblColNameMetaData = metaDataManager.getMetaData(strTableName);
        if (!htblColNameValue.keySet().equals(htblColNameMetaData.keySet()))
            throw new DBSchemaException("Column names do not match table schema");
        if (!Validation.validateSchema(htblColNameValue, htblColNameMetaData))
            throw new DBSchemaException("Columns metadata do not match table schema");

        Table table = serializationManager.deserializeTable(strTableName);
        table.deleteTuples(htblColNameValue);

        serializationManager.serializeTable(table);
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        return null;
    }

}
