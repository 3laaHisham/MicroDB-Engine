package utils;

import exceptions.DBAlreadyExistsException;
import exceptions.DBAppException;
import exceptions.DBNotFoundException;

import java.io.*;
import java.util.Hashtable;

public class MetaDataManager {
    private static final String META_DATA_FOLDER = "src/main/resources/metadata/";


    // Delete all metadata files and create a new folder
    public MetaDataManager() throws IOException {
        File metaFolder = new File(META_DATA_FOLDER);

        if (metaFolder.exists())
            Utils.deleteFolder(metaFolder);

        if (!metaFolder.mkdirs())
            throw new IOException("Failed to create metadata folder");
    }


    public void createTableMetaData(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
                                    Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {

        String tableMetaDataFile = META_DATA_FOLDER + strTableName + ".csv";
        if (new File(tableMetaDataFile).exists())
            throw new DBAlreadyExistsException("Table MetaData already exists");

        FileWriter writer = new FileWriter(tableMetaDataFile, true);
        writer.write("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,Min,Max\n");

        // loop on all columns and write their metadata to the file
        int numCols = htblColNameType.size();
        String[] ColNames = htblColNameType.keySet().toArray(new String[numCols]);
        for (int i = 0; i < numCols; i++) {
            String ColNameType = htblColNameType.get(ColNames[i]);
            String ColNameMin = htblColNameMin.get(ColNames[i]);
            String ColNameMax = htblColNameMax.get(ColNames[i]);

            writer.write(strTableName + "," + ColNames[i] + "," + ColNameType + "," + (ColNames[i].equals(strClusteringKeyColumn) ? "True" : "False")
                    + "," + "null" + "," + "null" + "," + ColNameMin + "," + ColNameMax + (i != numCols - 1 ? "\n" : ""));
        }
        writer.close();
        System.out.println("Table MetaData created successfully ar " + tableMetaDataFile);
    }


    // getMetaData
    // returns hashtable of String key for (column name) and hashtable value
    // that have all data about column (type, isClusteringKey, indexName, indexType, min, max) and values
    public Hashtable<String, Hashtable<String, String>> getMetaData(String strTableName)
            throws IOException, DBAppException {

        String tableMetaDataFile = META_DATA_FOLDER + strTableName + ".csv";
        if (!(new File(tableMetaDataFile).exists()))
            throw new DBNotFoundException("Table MetaData does not exist");

        // read the csv file and return the data
        FileReader fr = new FileReader(tableMetaDataFile);
        BufferedReader br = new BufferedReader(fr);
        Hashtable<String, Hashtable<String, String>> htblTableMetaData = new Hashtable<>();

        // read the header and then insert into hashtable (key: column name, value: hashtable of column metadata)
        String[] header = br.readLine().split(",");
        while (br.ready()) {
            String[] colMetaData = br.readLine().split(","); // array Column metadata

            Hashtable<String, String> htblColMetaData = new Hashtable<>();
            for (int i = 0; i < header.length; i++)
                htblColMetaData.put(header[i], colMetaData[i]); // Example: htblColMetaData.put("ColumnType", Double)

            String colName = colMetaData[1];
            htblTableMetaData.put(colName, htblColMetaData); // put (Column Name, hashtable Column Metadata)
        }
        br.close();

        return htblTableMetaData;
    }


    public static Hashtable<String, String> getClusteringKeyMetaData(Hashtable<String, Hashtable<String, String>> htblColNameMetaData) {
        for (Hashtable<String, String> htblColMetaData : htblColNameMetaData.values())
            if (htblColMetaData.get("ClusteringKey").equals("True"))
                return htblColMetaData;
        return null;
    }

    public void createIndex(String strTableName, String[] strarrColName){
        String tableMetaDataFile = META_DATA_FOLDER + strTableName + ".csv";
        
        if (new File(tableMetaDataFile).exists())
            throw new DBAlreadyExistsException("Table MetaData already exists");


        FileWriter writer = new FileWriter(tableMetaDataFile, true);
        writer.write("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,Min,Max\n");
        FileReader fr = new FileReader(tableMetaDataFile);
        BufferedReader br = new BufferedReader(fr);
        String[] header = br.readLine().split(",");


        int numCols = strarrColName.size();;
        
        while(br.ready()){
            int i = 0;
            String[] colMetaData = br.readLine().split(",");
            String ColNameType = htblColNameType.get(colMetaData[2]);
            String ColNameMin = htblColNameMin.get(colMetaData[7]);
            String ColNameMax = htblColNameMax.get(colMetaData[8]);
            if(strarrColName[i].equals(colMetaData[1])){
                writer.write(strTableName + "," + colMetaData[1] + "," + ColNameType + "," + (colMetaData[1].equals(strClusteringKeyColumn) ? "True" : "False")
                + "," + index.getindexname() + "," + "OCtree" + "," + ColNameMin + "," + ColNameMax + (i != numCols - 1 ? "\n" : ""));
            }esle{
                writer.write(strTableName + "," + colMetaData[1] + "," + ColNameType + "," + (colMetaData[1].equals(strClusteringKeyColumn) ? "True" : "False")
                + "," + "null" + "," + "null" + "," + ColNameMin + "," + ColNameMax + (i != numCols - 1 ? "\n" : ""));
            }
           
        fr.delete();
        writer.close();
        
    }

}
