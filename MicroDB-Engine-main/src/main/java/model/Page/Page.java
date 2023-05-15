package model.Page;

import exceptions.DBAlreadyExistsException;
import exceptions.DBAppException;
import exceptions.DBNotFoundException;
import model.Tuple;
import utils.Utils;

import java.io.IOException;
import java.util.Vector;

public class Page extends AbstractPage {
    private Vector<Tuple> tuples; // allows only Tuple, sorted by clusterKey
    private transient PageReference pageReference; // transient, to be set when deserializing from Table.pagesReference

    public Page(String tableName, int pageIndex) {
        super(tableName, pageIndex);
        this.tuples = new Vector<>();
        this.pageReference = new PageReference(tableName, pageIndex);
    }

    public Tuple findTuple(Object clusterKeyValue) throws DBAppException {
        int index = Utils.binarySearch(tuples, clusterKeyValue);

        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");
        return this.tuples.get(index);
    }

    public void insertTuple(Tuple tuple) throws DBAppException {
        int index = Utils.binarySearch(tuples, tuple);
        if (index >= 0)
            throw new DBAlreadyExistsException("Tuple already exists");

        int insertionIndex = Utils.getInsertionIndex(index);
        this.tuples.add(insertionIndex, tuple);

        updateMinMaxSize();
    }

    public void deleteTuple(Tuple tuple) throws DBAppException {
        int index = Utils.binarySearch(tuples, tuple);
        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");

        this.tuples.remove(index);

        updateMinMaxSize();
    }

    public void updateTuple(Tuple tuple) throws DBAppException {
        int index = Utils.binarySearch(tuples, tuple);
        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");

        // No need to sort again, since updateTable will not update clusterKey
        this.tuples.set(index, tuple);

        updateMinMaxSize();
    }

    // Helper Method
    public void updateMinMaxSize() {
        setSize(tuples.size());
        setMin(getSize() == 0 ? null : getMinTuple().getClusterKeyValue());
        setMax(getSize() == 0 ? null : getMaxTuple().getClusterKeyValue());

        this.pageReference.setSize(getSize());
        this.pageReference.setMin(getMin());
        this.pageReference.setMax(getMax());
    }

    public String toString() {
        try {
            int pageIndex = getPageIndex();
            int pageMaxSize = Utils.getMaxRowsCountInPage();

            String s = "Page " + (pageIndex + 1) + ":\n";
            for (int i = 0; i < getSize(); i++)
                s += (pageIndex*pageMaxSize + i + 1) + ". " + getTuple(i).toString() + "\n";
            return s;
        } catch (IOException e) {
            return null;
        }

    }


    public Tuple getTuple(int index) {
        return this.tuples.get(index);
    }

    public PageReference getPageReference() {
        return this.pageReference;
    }

    public void setPageReference(PageReference pageReference) {
        this.pageReference = pageReference;
    }

    public Tuple getMaxTuple() {
        return getTuple(getSize() - 1);
    }

    public Tuple getMinTuple() {
        return getTuple(0);
    }
}


