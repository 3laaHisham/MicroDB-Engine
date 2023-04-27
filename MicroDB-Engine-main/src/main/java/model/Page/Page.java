package model.Page;

import exceptions.DBAlreadyExistsException;
import exceptions.DBAppException;
import exceptions.DBNotFoundException;
import model.Tuple;
import utils.Utils;

import java.util.Vector;

public class Page extends AbstractPage {
    private Vector<Comparable> tuples; // allows only Tuple, sorted by clusterKey
    private PageReference pageReference;

    public Page(String tableName, int pageIndex) {
        super(tableName, pageIndex);
        this.tuples = new Vector<>();
        this.pageReference = new PageReference(tableName, pageIndex);
    }


    public Tuple getTuple(Object clusterKeyValue) throws DBAppException {
        Comparable searchKey = (Comparable) clusterKeyValue; // contains only clusterKeyValue to be used in search
        int index = Utils.binarySearch(tuples, searchKey);

        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");

        return (Tuple) tuples.get(index);
    }

    public void insertTuple(Tuple tuple) throws DBAppException {
        int index = Utils.binarySearch(tuples, tuple);
        if (index >= 0)
            throw new DBAlreadyExistsException("Tuple already exists");
        int insertionIndex = Utils.getInsertionIndex(index);

        tuples.add(insertionIndex, tuple);

        this.updateMinMaxSize();
    }

    public void deleteTuple(Tuple tuple) throws DBAppException {
        int index = Utils.binarySearch(tuples, tuple);
        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");

        tuples.remove(index);

        this.updateMinMaxSize();
    }

    public void updateTuple(Tuple t) throws DBAppException {
        int index = Utils.binarySearch(tuples, t);
        if (index < 0)
            throw new DBNotFoundException("Tuple does not exist");

        // No need to sort again, since updateTable will not update clusterKey
        tuples.set(index, t);

        this.updateMinMaxSize();
    }

    // Helper Methods
    public void updateMinMaxSize() {
        this.setSize(tuples.size());
        this.setMin(getMinTuple().getClusterKeyValue());
        this.setMax(getMaxTuple().getClusterKeyValue());

        pageReference.setSize(this.getSize());
        pageReference.setMin(this.getMin());
        pageReference.setMax(this.getMax());
    }

    public PageReference getPageReference() {
        return this.pageReference;
    }

    public Tuple getMaxTuple() {
        return (Tuple) tuples.get(getSize() - 1);
    }

    public Tuple getMinTuple() {
        return (Tuple) tuples.get(0);
    }
}
