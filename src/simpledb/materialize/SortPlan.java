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
public class SortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;

    /**
     * Creates a sort plan for the specified query.
     *
     * @param p the plan for the underlying query
     * @param sortfields the fields to sort by
     * @param tx the calling transaction
     */
    public SortPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
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
        while (copy(scan, currentScan)) {
            if (comp.compare(scan, currentScan) < 0) {
                // start a new run
                currentScan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentScan = (UpdateScan) currenttemp.open();
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
        while (hasmore1 && hasmore2) {
            System.out.println("Hey you : " + comp.compare(src1, src2));
            System.out.println("src1 " + src1.getVal("Sname") + ", src2 " + src2.getVal("Sname"));
            int solution = comp.compare(src1, src2);
            //http://www.leepoint.net/data/expressions/22compareobjects.html
//            if (solution != 0) {
                if (comp.compare(src1, src2) < 0) {
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore2 = copy(src2, dest);
                }
//            }
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

    protected boolean copy(Scan scan, UpdateScan newUpdateScanObject) {
        newUpdateScanObject.insert();
        for (String fldname : sch.fields()) {
            newUpdateScanObject.setVal(fldname, scan.getVal(fldname));
        }
        return scan.next();
    }
}
