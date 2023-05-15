package model;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;

public class Tuple implements Comparable, Serializable {
    private final String clusterKeyName;
    private final Hashtable<String, Object> htblColNameValue;

    public Tuple(String clusterKeyName, Hashtable<String, Object> htblColNameValue) {
        this.clusterKeyName = clusterKeyName;
        this.htblColNameValue = htblColNameValue;
    }

    public Set<String> getColNames() {
        return htblColNameValue.keySet();
    }

    public String getClusterKeyName() {
        return clusterKeyName;
    }

    public Object getColValue(String colName) {
        return htblColNameValue.get(colName);
    }

    public void setColValue(String colName, Object value) {
        htblColNameValue.put(colName, value);
    }

    public Object getClusterKeyValue() {
        return htblColNameValue.get(clusterKeyName);
    }

    public String toString() {
        String s = "";
        for (String colName : htblColNameValue.keySet())
            s += colName + ": " + htblColNameValue.get(colName) + ", ";
        return s;
    }


    // compareTo based on some column name
    public int compareTo(Tuple o, String colName) {
        Comparable thisValue = (Comparable) this.getColValue(colName);
        Object otherValue = o.getColValue(colName);

        return thisValue.compareTo(otherValue);
    }

    @Override
    public int compareTo(Object o) {
        Comparable thisValue = (Comparable) getClusterKeyValue();
        Object otherValue;

        if (o instanceof Tuple)
            otherValue = ((Tuple) o).getClusterKeyValue();
        else
            // If o is not a tuple, it is a clusterKeyValue
            otherValue = o;

        return thisValue.compareTo(otherValue);
    }

}