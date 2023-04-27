package model;

import exceptions.DBAppException;
import model.Page.Page;
import model.Page.PageReference;
import utils.SerializationManager;
import utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private final Vector<Comparable> pagesReference;
    private final String tableName;
    private final String clusterKeyName;
    private int size;
    private transient SerializationManager serializationManager;

    public Table(String tableName, String clusterKeyName) {
        this.pagesReference = new Vector<>();
        this.tableName = tableName;
        this.clusterKeyName = clusterKeyName;
        this.size = 0;

        String tableFolder = Utils.getPageFolderPath(tableName);
        Utils.createFolder(tableFolder);
    }

    public void insertTuple(Tuple tuple) throws DBAppException, IOException {
        if (this.getPagesCount() == 0 || this.isFull()) // If no pages exist OR table is full
            this.addPage(new Page(this.tableName, getPagesCount()));

        Comparable clusterKeyValue = (Comparable) tuple.getClusterKeyValue();
        int pageIndex = this.getInsertionPageIndex(clusterKeyValue);

        Page page = serializationManager.deserializePage(this.tableName, pageIndex);
        page.insertTuple(tuple);

        serializationManager.serializePage(page);

        this.size++;
        arrangePages();
    }

    public void deleteTuples(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        for (int i = 0; i < getPagesCount(); i++) {
            Page page = serializationManager.deserializePage(this.tableName, i);

            Vector<Tuple> toDelete = matchesCriteria(page, htblColNameValue);

            for (Tuple tuple : toDelete)
                page.deleteTuple(tuple);

            serializationManager.serializePage(page);
        }

        arrangePages();
    }

    private Vector<Tuple> matchesCriteria(Page page, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Vector<Tuple> toDelete = new Vector<>();
        for (int j = 0; j < page.getSize(); j++) {
            Tuple tuple = page.getTuple(j);
            for (String key : htblColNameValue.keySet())
                if (!tuple.getColValue(key).equals(htblColNameValue.get(key)))
                    break;
            toDelete.add(tuple);

            this.size--;
        }

        return toDelete;
    }

    // returns page where this clusterKeyValue is between min and max
    private int getInsertionPageIndex(Comparable clusterKeyValue) {
        int index = Utils.binarySearch(pagesReference, clusterKeyValue);
        if (index < 0) // If not between any page's min-max, get page index where it would be the new min
            index = Utils.getInsertionIndex(index);
        if (index > this.getPagesCount() - 1) // If index is out of bounds (clusterKeyValue is greatest)
            index = this.getPagesCount() - 1;

        return index;
    }

    private void arrangePages() throws IOException, DBAppException {
        this.distributePages();

        this.removeEmptyPages();
    }

    // It is guaranteed that there are enough pages to distribute tuples
    private void distributePages() throws IOException, DBAppException {
        int n = this.getPagesCount();
        for (int i = 0; i < n - 1; i++) {
            PageReference currPageRef = getPageReference(i);
            PageReference nextPageRef = getPageReference(i + 1);

            if (currPageRef.isOverflow()) { // Shift 1 tuple to next page
                int numShifts = 1;
                shiftTuplesTo(currPageRef, nextPageRef, numShifts);
            }
            if (!currPageRef.isFull() && !nextPageRef.isEmpty()) { // Shift tuples from next page to current page to fill space
                int numShifts = currPageRef.getEmptySpace();
                shiftTuplesTo(currPageRef, nextPageRef, numShifts);
            }
        }
    }

    private void removeEmptyPages() throws IOException, DBAppException {
        int n = getPagesCount();
        for (int i = 0; i < n; i++) {
            PageReference currPageRef = this.getPageReference(i);
            if (currPageRef.isEmpty())
                removePage(currPageRef);
        }
    }

    private void shiftTuplesTo(PageReference fromPageRef, PageReference toPageRef, int numShifts) throws DBAppException, IOException {
        Page fromPage = serializationManager.deserializePage(this.tableName, fromPageRef.getPageIndex());
        Page toPage = serializationManager.deserializePage(this.tableName, toPageRef.getPageIndex());

        Tuple tuple;
        int n = fromPage.getSize();
        for (int i = 0; i < numShifts && i < n; i++) {
            if (toPage.getPageIndex() > fromPage.getPageIndex())
                tuple = fromPage.getMaxTuple();
            else
                tuple = fromPage.getMinTuple();
            fromPage.deleteTuple(tuple);
            toPage.insertTuple(tuple);
        }

        serializationManager.serializePage(fromPage);
        serializationManager.serializePage(toPage);
    }


    private void addPage(Page page) throws IOException {
        PageReference pageReference = page.getPageReference();
        this.pagesReference.add(pageReference);

        serializationManager.serializePage(page);
    }

    private void removePage(PageReference pageReference) throws IOException {
        this.pagesReference.remove(pageReference);

        File pageFile = new File(pageReference.getPagePath());
        Utils.deleteFolder(pageFile);
    }


    public String getPagePath(int pageIndex) {
        PageReference pageReference = (PageReference) this.pagesReference.get(pageIndex);
        String pagePath = pageReference.getPagePath();

        return pagePath;
    }

    public PageReference getPageReference(int pageIndex) {
        return (PageReference) this.pagesReference.get(pageIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public String getClusterKeyName() {
        return clusterKeyName;
    }

    public int getPagesCount() {
        return this.pagesReference.size();
    }

    public boolean isFull() throws IOException {
        return size >= Utils.getMaxRowsCountInPage() * getPagesCount();
    }

    public int getSize() {
        return size;
    }

    public void setSerializationManager(SerializationManager serializationManager) {
        this.serializationManager = serializationManager;
    }
}
