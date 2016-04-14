package simpledb.materialize;

import java.util.List;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class NoDupsSortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private List<String> indices;
    private Schema sch = new Schema();
    private RecordComparator comp;
    private List<TempTable> runs;

    public NoDupsSortPlan(Plan p, List<String> indices, Schema schema, Transaction tx) {
        this.indices = indices;
        this.p = new SortPlan(p, indices, tx);
        this.sch = schema;
    }

//    @Override
//    public Scan open() {
//        Scan src = p.open();
//        List<TempTable> runs = splitIntoRuns(src);
//        src.close();
//        while (runs.size() > 2) {
//            runs = doAMergeIteration(runs);
//        }
//        return new SortScan(runs, comp);
//    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : indices) {
            numgroups *= p.distinctValues(fldname);
        }
        return numgroups;
    }

    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname)) {
            return p.distinctValues(fldname);
        } else {
            return recordsOutput();
        }
    }

    public Schema schema() {
        return sch;
    }

    @Override
    public Scan open() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
