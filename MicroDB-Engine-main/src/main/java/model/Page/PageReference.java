package model.Page;

// implements Comparable to be able to use binarySearch
public class PageReference extends AbstractPage implements Comparable {

    // Access modifier is default to prevent initialization from outside the package
    PageReference(String tableName, int pageIndex) {
        super(tableName, pageIndex);
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof PageReference) {
            Comparable thisValue = (Comparable) getMin();
            Object otherValue = ((PageReference) o).getMin();

            return thisValue == null ? 0 : thisValue.compareTo(otherValue); // thisValue == null if page is empty
        } else {
            // If o is not a PageReference, it is a clusterKeyValue
            Comparable thisValueMin = (Comparable) getMin();
            Comparable thisValueMax = (Comparable) getMax();
            Object otherValue = o;

            if (thisValueMin == null || thisValueMax == null) // thisValue == null if page is empty
                return 0;
            if (thisValueMin.compareTo(otherValue) <= 0 && thisValueMax.compareTo(otherValue) >= 0)
                return 0;
            else if (thisValueMin.compareTo(otherValue) > 0)
                return 1;
            else
                return -1;
        }

    }
}

