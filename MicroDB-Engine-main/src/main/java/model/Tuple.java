package model;

import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Comparable, Serializable {
    private final String clusterKeyName;
    private Hashtable<String, Object> htblColNameValue;

    public Tuple(String clusterKeyName, Hashtable<String, Object> htblColNameValue) {
        this.clusterKeyName = clusterKeyName;
        this.htblColNameValue = htblColNameValue;
    }

    // Creates a tuple with clusterKeyValue as needed to help sort based on ClusterKeyColumn using compareTo
    public static Tuple createInstance(Object clusterKeyValue) {
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("whateverKey", clusterKeyValue);

        return new Tuple("whateverKey", htblColNameValue);

    }

    public String getClusterKeyName() {
        return clusterKeyName;
    }

    public Object getColValue(String colName) {
        return htblColNameValue.get(colName);
    }

    public Object getClusterKeyValue() {
        return htblColNameValue.get(clusterKeyName);
    }

    @Override
    public int compareTo(Object o) {
        Comparable thisValue = (Comparable) this.getClusterKeyValue();
        Comparable otherValue;

        if (o instanceof Tuple)
            otherValue = (Comparable) ((Tuple) o).getClusterKeyValue();
        else
            // If o is not a tuple, it is a clusterKeyValue
            otherValue = (Comparable) o;

        return thisValue.compareTo(otherValue);
    }

    // compareTo based on some column name
    public int compareTo(Tuple o, String colName) {
        Comparable thisValue = (Comparable) this.getColValue(colName);
        Comparable otherValue = (Comparable) o.getColValue(colName);
        return thisValue.compareTo(otherValue);
    }

}
