
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class IndexManager {
    private List<Index> indices;

    public IndexManager() {
        indices = new ArrayList<>();
    }

    public void addIndex(Index index) {
        if (!indices.contains(index)) {
            indices.add(index);
        }
    }

    public void removeIndex(Index index) {
        indices.remove(index);
    }

    public List<Index> getIndices() {
        return indices;
    }
    public static Index combineIndices(Set<Index> indices) {
        if (indices.size() < 2) {
            return null;
        }
    
        List<String[]> indexColumns = new ArrayList<>();
        List<Hashtable<String, Object>> indexRanges = new ArrayList<>();
    
        for (Index index : indices) {
            indexColumns.add(index.getColumnNames());
            indexRanges.add(index.getRange());
        }
    
        String[] combinedColumns = combineColumns(indexColumns);
        Hashtable<String, Object> combinedRange = combineRanges(indexRanges);
    
        if (combinedColumns == null || combinedRange == null) {
            return null;
        }
    
        return new Index(combinedColumns, combinedRange);
    }
    
    private static String[] combineColumns(List<String[]> indexColumns) {
        // Combine the column names from all indices into a single array
        Set<String> combinedColumnsSet = new HashSet<>();
        for (String[] columns : indexColumns) {
            combinedColumnsSet.addAll(Arrays.asList(columns));
        }
    
        String[] combinedColumns = new String[combinedColumnsSet.size()];
        return combinedColumnsSet.toArray(combinedColumns);
    }
    
    private static Hashtable<String, Object> combineRanges(List<Hashtable<String, Object>> indexRanges) {
        // Combine the ranges from all indices into a single range
        Hashtable<String, Object> combinedRange = new Hashtable<>();
        for (Hashtable<String, Object> range : indexRanges) {
            combinedRange.putAll(range);
        }
    
        return combinedRange;
    }
    
    public Set<Tuple> getMatchingTuples(Hashtable<String, Object> partialQuery, Table table) {
        Set<Tuple> matchingTuples = new HashSet<>();

        for (Index index : indices) {
            Set<Integer> matchingPages = index.getPagesIndex(partialQuery);

            for (Integer pageIndex : matchingPages) {
                PageReference pageReference = table.getPageReference(pageIndex);
                Page page = pageReference.getPage();

                Vector<Tuple> tuples = page.getTuples();
                for (Tuple tuple : tuples) {
                    if (tuple.matchesPartialQuery(partialQuery)) {
                        matchingTuples.add(tuple);
                    }
                }
            }
        }

        return matchingTuples;
    }
}
