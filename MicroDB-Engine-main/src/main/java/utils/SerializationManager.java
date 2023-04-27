package utils;

import exceptions.DBNotFoundException;
import model.Page.Page;
import model.Table;

import java.io.*;

public class SerializationManager {
    private final String TABLES_DATA_FOLDER = "src/main/resources/Tables/";
    private final String PAGES_Table_FOLDER = "Pages/";

    // Delete all tables files and create a new folder
    public SerializationManager() throws IOException {
        File TablesFolder = new File(TABLES_DATA_FOLDER);

        if (TablesFolder.exists())
            Utils.deleteFolder(TablesFolder);

        if (!TablesFolder.mkdirs())
            throw new IOException("Failed to create Tables folder");
    }


    public void serializeTable(Table table) throws IOException {
        String tableName = table.getTableName();
        String tablePath = TABLES_DATA_FOLDER + tableName + "/" + tableName + ".ser";

        serialize(table, tablePath);
    }

    public Table deserializeTable(String strTableName) throws IOException, DBNotFoundException {
        String tablePath = TABLES_DATA_FOLDER + strTableName + "/" + strTableName + ".ser";

        Table table = (Table) deserialize(tablePath);
        table.setSerializationManager(this);

        return table;
    }

    public void serializePage(Page page) throws IOException {
        String tableName = page.getTableName();
        int pageIndex = page.getPageIndex();
        String PagePath = TABLES_DATA_FOLDER + tableName + "/" + PAGES_Table_FOLDER + pageIndex + ".ser";

        serialize(page, PagePath);
    }

    public Page deserializePage(String strTableName, int pageIndex) throws IOException, DBNotFoundException {
        String PagePath = TABLES_DATA_FOLDER + strTableName + "/" + PAGES_Table_FOLDER + pageIndex + ".ser";
        Page page = (Page) deserialize(PagePath);

        return page;
    }


    // Helper methods
    private void serialize(Object obj, String filePath) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(filePath);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);

        out.writeObject(obj);

        out.close();
        fileOut.close();
    }

    private Object deserialize(String filePath) throws IOException, DBNotFoundException {
        FileInputStream fileIn = new FileInputStream(filePath);
        ObjectInputStream in = new ObjectInputStream(fileIn);

        Object obj;
        try {
            obj = (Object) in.readObject();
        } catch (Exception e) {
            throw new DBNotFoundException("Table not found");
        }

        in.close();
        fileIn.close();

        return obj;
    }

}
