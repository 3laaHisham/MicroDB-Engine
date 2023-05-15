package model;

import exceptions.DBAppException;
import exceptions.DBNotFoundException;
import exceptions.DBQueryException;
import model.Page.Page;
import model.Page.PageReference;
import utils.SerializationManager;
import utils.Utils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private final Vector<PageReference> pagesReference;
    private final Vector<Index> indices;
    private final String tableName;
    private final String clusterKeyName;
    private int size;

    public Table(String tableName, String clusterKeyName) {
        this.pagesReference = new Vector<>();
        this.indices = new Vector<>();
        this.tableName = tableName;
        this.clusterKeyName = clusterKeyName;
        this.size = 0;

        String pagesFolder = Utils.getPageFolderPath(tableName);
        String IndexFolder = Utils.getIndexFolderPath(tableName);
        Utils.createFolder(pagesFolder);
        Utils.createFolder(IndexFolder);
    }

    public void insertTuple(Tuple tuple) throws DBAppException {
        valadation.validateColumnTypes(tuple);
        if (this.getPagesCount() == 0 || this.isFull()) // If no pages exist OR table is full
            addPage(new Page(this.tableName, getPagesCount()));

        Object clusterKeyValue = tuple.getClusterKeyValue();
        int index = Utils.binarySearch(this.pagesReference, clusterKeyValue);
        int pageIndex = getInsertionPageIndex(index);

        PageReference pageRef = getPageReference(pageIndex);
        Page page = SerializationManager.deserializePage(getTableName(), pageRef);
        page.insertTuple(tuple);

        SerializationManager.serializePage(page);

        this.size++;
        arrangePages();
    }

    public void deleteTuples(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Page page;
        for (PageReference pageRef : pagesReference) {
            page = SerializationManager.deserializePage(getTableName(), pageRef);

            Vector<Tuple> toDelete = matchesCriteria(page, htblColNameValue);

            for (Tuple tuple : toDelete) {
                page.deleteTuple(tuple);
                this.size--;
            }

            SerializationManager.serializePage(page);
        }

        arrangePages();
    }

    public void updateTuple(Object clusterKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        
    Tuple tuple = findTuple(clusterKeyValue);


    valadation.validateColumnTypes(tuple, htblColNameValue);
        int pageIndex = Utils.binarySearch(this.pagesReference, clusterKeyValue);
        if (pageIndex < 0)
            throw new DBNotFoundException("Tuple does not exist");

        PageReference pageRef = getPageReference(pageIndex);
        Page page = SerializationManager.deserializePage(getTableName(), pageRef);
        for (String key : htblColNameValue.keySet()) {
            if (key.equals(this.clusterKeyName))
                throw new DBQueryException("Cannot update cluster key value");
            tuple.setColValue(key, htblColNameValue.get(key));
        }

//        page.updateTuple(tuple);

        SerializationManager.serializePage(page);
    }
    public Index getCombinedIndex(LinkedHashMap<String, Object> htblColNameValue) {
        // Get the relevant indices for the columns in the query
        Set<Index> relevantIndices = getRelevantIndices(htblColNameValue.keySet());

        // Combine the relevant indices into a single index
        Index combinedIndex = IndexManager.combineIndices(relevantIndices);

        return combinedIndex;
    }

    private Set<Index> getRelevantIndices(Set<String> columnNames) {
        Set<Index> relevantIndices = new HashSet<>();

        for (Index index : indices) {
            String[] indexColNames = index.getColNames();
            Set<String> indexColumns = new HashSet<>(Arrays.asList(indexColNames));
            if (indexColumns.containsAll(columnNames)) {
                relevantIndices.add(index);
            }
        }

        return relevantIndices;
    }
    public boolean hasIndexForColumns(String[] columnNames) {
        for (Index index : indexes) {
            String[] indexColNames = index.getColNames();
            if (Arrays.equals(columnNames, indexColNames)) {
                return true;
            }
        }
        return false;
    }
    
        public Iterator selectTuples(LinkedHashMap<String, Object> htblColNameValue, String[] compareOperators, String[] strarrOperators) throws DBAppException {
            valadation.validateColumnTypes(htblColNameValue);
            List<Tuple> tuples = new ArrayList<>();
        
            Page page;
            for (PageReference pageRef : pagesReference) {
                page = SerializationManager.deserializePage(getTableName(), pageRef);
                for (int j = 0; j < page.getSize(); j++) {
                    Tuple tuple = page.getTuple(j);
        
                    Boolean[] matches = new Boolean[htblColNameValue.size()];
                    Arrays.fill(matches, true);
        
                    Object[] keySet = htblColNameValue.keySet().toArray();
                    for (int k = 0; k < keySet.length; k++) {
                        String key = (String) keySet[k];
                        if (!isConditionTrue(tuple.getColValue(key), htblColNameValue.get(key), compareOperators[k]))
                            matches[k] = false;
                    }
        
                    if (isTupleSatisfy(matches, strarrOperators))
                        tuples.add(tuple);
                }
            }
        
            return tuples.iterator();
        }
        public boolean validateTuple(Tuple tuple, LinkedHashMap<String, Object> htblColNameValue, String[] compareOperators, String[] strarrOperators) {
            Boolean[] matches = new Boolean[htblColNameValue.size()];
            Arrays.fill(matches, true);
        
            Object[] keySet = htblColNameValue.keySet().toArray();
            for (int k = 0; k < keySet.length; k++) {
                String key = (String) keySet[k];
                if (!isConditionTrue(tuple.getColValue(key), htblColNameValue.get(key), compareOperators[k]))
                    matches[k] = false;
            }
        
            return isTupleSatisfy(matches, strarrOperators);
        }
        
        private boolean isConditionTrue(Object tupleValue, Object queryValue, String operator) {
            if (operator.equals("="))
                return Objects.equals(tupleValue, queryValue);
            else if (operator.equals(">"))
                return compareValues(tupleValue, queryValue) > 0;
            else if (operator.equals("<"))
                return compareValues(tupleValue, queryValue) < 0;
            else if (operator.equals(">="))
                return compareValues(tupleValue, queryValue) >= 0;
            else if (operator.equals("<="))
                return compareValues(tupleValue, queryValue) <= 0;
            else if (operator.equals("!="))
                return !Objects.equals(tupleValue, queryValue);
            else
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
        
        private int compareValues(Object value1, Object value2) {
            if (value1 instanceof Comparable && value2 instanceof Comparable) {
                return ((Comparable) value1).compareTo(value2);
            } else {
                throw new IllegalArgumentException("Values are not comparable");
            }
        }
        
        private boolean isTupleSatisfy(Boolean[] conditionsBool, String[] betweenConditions) {
            if (betweenConditions.length == 0)
                return conditionsBool[0];
        
            boolean result = conditionsBool[0];
            for (int i = 1; i < conditionsBool.length; i++) {
                String operator = betweenConditions[i - 1].toLowerCase();
        
                if (operator.equals("and"))
                    result = result && conditionsBool[i];
                else if (operator.equals("or"))
                    result = result || conditionsBool[i];
                else if (operator.equals("xor"))
                    result = result ^ conditionsBool[i];
                else
                    throw new IllegalArgumentException("Invalid logical operator: " + operator);
            }
        
            return result;
        }
            
        public Iterator selectTuplesWithIndex(LinkedHashMap<String, Object> htblColNameValue, String[] compareOperators, String[] strarrOperators) throws DBAppException {
            // Get the relevant indices for the queried columns
            Set<String> queryColumns = htblColNameValue.keySet();
            Set<Index> relevantIndices = getRelevantIndices(queryColumns);
        
            // Check if a combined index is available for the query columns
            Index combinedIndex = null;
            if (relevantIndices.size() > 0) {
                combinedIndex = IndexManager.combineIndices(relevantIndices);
                if (combinedIndex != null) {
                    // Use the combined index to retrieve matching tuples
                    Set<Tuple> selectedTuples = combinedIndex.getMatchingTuples(htblColNameValue);
                    return selectedTuples.iterator();
                }
            }
        
            // No combined index available, fall back to linear scanning on the table
            List<Tuple> selectedTuples = new ArrayList<>();
        
            Page page;
            for (PageReference pageRef : pagesReference) {
                page = SerializationManager.deserializePage(getTableName(), pageRef);
                for (int j = 0; j < page.getSize(); j++) {
                    Tuple tuple = page.getTuple(j);
        
                    // Check if the tuple satisfies the query conditions
                    if (validateTuple(tuple, htblColNameValue, compareOperators, strarrOperators)) {
                        selectedTuples.add(tuple);
                    }
                }
            }
        
            return selectedTuples.iterator();
        }
        
    
    public void createIndex(String[] ColNames, Hashtable<String, Object> min, Hashtable<String, Object> max) throws DBAppException {
        this.indices.add(new Index(ColNames, min, max));
        
    }

    private Vector<Tuple> matchesCriteria(Page page, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Vector<Tuple> toDelete = new Vector<>();
        Tuple tuple;
        for (int j = 0; j < page.getSize(); j++) {
            tuple = page.getTuple(j);

            boolean matches = true;
            for (String key : htblColNameValue.keySet())
                if (!tuple.getColValue(key).equals(htblColNameValue.get(key)))
                    matches = false;

            if (matches)
                toDelete.add(tuple);
        }

        return toDelete;
    }

    // returns page where this clusterKeyValue is between min and max
    private int getInsertionPageIndex(int index) {
        if (index < 0) // If not between any page's min-max, get page index where it would be the new min
            index = Utils.getInsertionIndex(index);
        if (index > getPagesCount() - 1) // If index is out of bounds (clusterKeyValue is greatest)
            index = getPagesCount() - 1;

        return index;
    }

    private void insertIntoIndex() {

    }

    private void removeFromIndex() {

    }

    private void arrangePages() throws DBAppException {
        distributePages();

        removeEmptyPages();
    }

    // It is guaranteed that there are enough pages to distribute tuples
    private void distributePages(/*int startIndex*/) throws DBAppException {
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
                shiftTuplesTo(nextPageRef, currPageRef, numShifts);
            }
        }
    }

    private void removeEmptyPages() {
        int n = getPagesCount();
        for (int i = 0; i < n; i++) {
            PageReference currPageRef = getPageReference(i);
            if (currPageRef.isEmpty())
                removePage(currPageRef);
        }
    }

    private void shiftTuplesTo(PageReference fromPageRef, PageReference toPageRef, int numShifts) throws DBAppException {
        Page fromPage = SerializationManager.deserializePage(this.tableName, fromPageRef);
        Page toPage = SerializationManager.deserializePage(this.tableName, toPageRef);

        Tuple tuple;
        int n = fromPage.getSize();
        for (int i = 0; i < numShifts && i < n; i++) {
            if (toPage.getPageIndex() > fromPage.getPageIndex())
                tuple = fromPage.getMaxTuple();
            else
                tuple = fromPage.getMinTuple();
            // update pageIndex in index
            fromPage.deleteTuple(tuple);
            toPage.insertTuple(tuple);
        }

        SerializationManager.serializePage(fromPage);
        SerializationManager.serializePage(toPage);
    }



    private void addPage(Page page) throws DBAppException {
        PageReference pageReference = page.getPageReference();
        this.pagesReference.add(pageReference);

        SerializationManager.serializePage(page);
    }

    private void removePage(PageReference pageReference) {
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
        return this.pagesReference.get(pageIndex);
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getClusterKeyName() {
        return this.clusterKeyName;
    }

    public int getPagesCount() {
        return this.pagesReference.size();
    }

    public int getSize() {
        return this.size;
    }

    public boolean isFull() throws DBQueryException {
        try {
            return this.size >= Utils.getMaxRowsCountInPage() * getPagesCount();
        } catch (IOException e) {
            throw new DBQueryException("Error while getting max rows count in page");
        }
    }
}
