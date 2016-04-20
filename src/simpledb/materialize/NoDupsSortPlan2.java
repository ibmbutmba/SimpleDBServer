package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class for the <i>sort</i> operator.
 *
 * @author Edward Sciore
 */
public class NoDupsSortPlan2 implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private String sortBy = null;
    private List<String> fields;

    /**
     * Creates a sort plan for the specified query.
     *
     * @param p the plan for the underlying query
     * @param sortfields the fields to sort by
     * @param tx the calling transaction
     */
    public NoDupsSortPlan2(Plan p, List<String> sortfields, Transaction tx, String sortby) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
        fields = sortfields;
        this.sortBy = sortby;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public String getSortBy() {
        return sortBy;
    }

    /**
     * This method is where most of the action is. Up to 2 sorted temporary
     * tables are created, and are passed into SortScan for final merging.
     *
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new SortScan(runs, comp);
    }

    /**
     * Returns the number of blocks in the sorted table, which is the same as it
     * would be in a materialized table. It does <i>not</i> include the one-time
     * cost of materializing and sorting the records.
     *
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    /**
     * Returns the number of records in the sorted table, which is the same as
     * in the underlying query.
     *
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Returns the number of distinct field values in the sorted table, which is
     * the same as in the underlying query.
     *
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the sorted table, which is the same as in the
     * underlying query.
     *
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan scan) {
        List<TempTable> temps = new ArrayList<TempTable>();
        scan.beforeFirst();
        if (!scan.next()) {
            return temps;
        }
        TempTable currenttemp = new TempTable(sch, tx);
        temps.add(currenttemp);
        UpdateScan currentScan = currenttemp.open();
        int value = 0;
        while (copy(scan, currentScan)) {

            if (getSortBy() == null) {
                value = comp.compare(scan, currentScan);
            } else {
                value = compare(scan, currentScan);
            }
            System.out.println("value is : " + value);
            if (value < 0) {
                // start a new run
                currentScan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentScan = (UpdateScan) currenttemp.open();
            } else if ( value == 0) {
                currentScan = (UpdateScan) currenttemp.open();
                scan.next();
                System.out.println("PLEASE REMVOE ME!!!!!");
            }
        }
        currentScan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<TempTable>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(sch, tx);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        int value = 0;
        while (hasmore1 && hasmore2) {
//            //http://www.leepoint.net/data/expressions/22compareobjects.html
            System.out.println("P1 " + src1.getVal(getSortBy()) + "sorted by: " + getSortBy());
            System.out.println("P2 " + src2.getVal(getSortBy()) + "sorted by: " + getSortBy());

            if (getSortBy() == null) {
                value = comp.compare(src1, src2);
            } else {
                value = compare(src1, src2);
            }
            System.out.println("value is : " + value);
            if (value != 0) {
                if (value < 0) {
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore2 = copy(src2, dest);
                }
            } else {
                System.out.println("REMOVE THEM!");
            }
        }

        if (hasmore1) {
            while (hasmore1) {
                hasmore1 = copy(src1, dest);
            }
        } else {
            while (hasmore2) {
                hasmore2 = copy(src2, dest);
            }
        }

        src1.close();

        src2.close();

        dest.close();
        return result;
    }

    public int compare(Scan s1, Scan s2) {
        for (String fldname : fields) {
            if (fldname.equals(getSortBy())) {
                Constant val1 = s1.getVal(fldname);
                Constant val2 = s2.getVal(fldname);
                System.out.println("1 is : " + s1.getVal(fldname));
                System.out.println("2 is : " + s2.getVal(fldname));
                int result = val1.compareTo(val2);
                if (result != 0) {
                    return result;
                }
            }
        }
        return 0;
    }

    protected boolean copy(Scan scan, UpdateScan newUpdateScanObject) {
        newUpdateScanObject.insert();
        for (String fldname : sch.fields()) {
            newUpdateScanObject.setVal(fldname, scan.getVal(fldname));
        }
        return scan.next();
    }
}
