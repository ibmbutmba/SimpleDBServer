package simpledb.materialize;

import java.util.Comparator;
import simpledb.query.Constant;
import simpledb.query.Scan;

public class NoDupsRecordComparator implements Comparator<Scan> {

    private String distinctField;

    public NoDupsRecordComparator(String distinctField) {
        this.distinctField = distinctField;
    }

    public int compare(Scan s1, Scan s2) {
        Constant val1 = s1.getVal(distinctField);
        Constant val2 = s2.getVal(distinctField);
        
        int result = val1.compareTo(val2);
        return result;
    }

}
