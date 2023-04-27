package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.Vector;

public class Utils {
    private static final String TABLES_DATA_FOLDER = "src/main/resources/Tables/";
    private static final String PAGES_TABLE_FOLDER = "Pages/";

    // returns the index of Object, if it is in the list; otherwise, (-(insertion point if it were to be + 1)).
    public static int binarySearch(Vector<Comparable> list, Comparable o) {
        Comparator<Comparable> c = new Comparator<Comparable>() {
            public int compare(Comparable t1, Comparable t2) {
                return t1.compareTo(t2);
            }
        };

        int index = Collections.binarySearch(list, o, c);
        return index;
    }

    public static int getInsertionIndex(int index) {
        if (index < 0)
            index = -(index + 1);
        return index;
    }

    public static int getMaxRowsCountInPage() throws IOException {
        Properties prop = new Properties();
        FileInputStream configPath = new FileInputStream("src/main/resources/DBApp.config");
        prop.load(configPath);

        return Integer.parseInt(prop.getProperty("MaximumRowsCountInTablePage"));
    }

    // Helper function to delete a folder
    public static void deleteFolder(File f) {
        if (f.isDirectory())
            for (File c : f.listFiles())
                deleteFolder(c);

        if (!f.delete())
            throw new AssertionError("Failed to delete file: " + f);
    }

    public static void createFolder(String folderPath) {
        File folder = new File(folderPath);
        folder.mkdirs();
    }

    public static String getTableFolderPath(String strTableName) {
        return TABLES_DATA_FOLDER + strTableName + "/";
    }

    public static String getTableFilePath(String strTableName) {
        return TABLES_DATA_FOLDER + strTableName + "/" + strTableName + ".ser";
    }

    public static String getPageFolderPath(String strTableName) {
        return TABLES_DATA_FOLDER + strTableName + "/" + PAGES_TABLE_FOLDER;
    }

    public static String getPageFilePath(String strTableName, int pageIndex) {
        return TABLES_DATA_FOLDER + strTableName + "/" + PAGES_TABLE_FOLDER + pageIndex + ".ser";
    }

}
