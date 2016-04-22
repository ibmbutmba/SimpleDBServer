package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

public class NoDupsSortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private List<String> fields;

    public NoDupsSortPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
        fields = sortfields;
    }

    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRunsHashing(src);//it can be changed to splitIntoRuns(src)
        src.close();
        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new NoDupsSortScan(runs, comp);
    }

    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRunsHashing(Scan scan1) {
        UpdateScan scan = (UpdateScan) scan1;
        List<TempTable> temps = new ArrayList<TempTable>();
        scan.beforeFirst();
        if (!scan.next()) {
            return temps;
        }
        TempTable currenttemp = new TempTable(sch, tx);
        temps.add(currenttemp);
        UpdateScan currentScan = currenttemp.open();
        int value = 0;
        int i;
        while (copy(scan, currentScan)) {
            i = 0;
            while (i < 5 && scan.getString("Sname").length() > i) {
                value += ("" + scan.getString("Sname").charAt(i)).compareTo("m");
                i++;
            }
            if (value < 0) {
                currentScan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentScan = (UpdateScan) currenttemp.open();

            } else {
                currentScan.delete();
            }
        }
        currentScan.close();
        return temps;
    }

    private List<TempTable> splitIntoRuns(Scan scan1) {
        UpdateScan scan = (UpdateScan) scan1;
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

            value = comp.compare(scan, currentScan);

            if (value < 0) {
//                System.out.println("The value is < 0. The values are: "
//                        + scan.getString("Sname")
//                        + " "
//                        + currentScan.getString("Sname"));
                currentScan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentScan = (UpdateScan) currenttemp.open();

            } else if (value == 0) {
//                System.out.println("The value is 0. The values are: "
//                        + scan.getString("Sname")
//                        + " "
//                        + currentScan.getString("Sname"));
                currentScan.delete();
                //If they are equal, just skip and continue to the next one
//                currentScan.close();
//                currentScan = (UpdateScan) currenttemp.open();
//                scan.next();
            } else {
//                System.out.println("The value is > 0. The values are: "
//                        + scan.getString("Sname")
//                        + " "
//                        + currentScan.getString("Sname"));
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
        UpdateScan src1 = (UpdateScan) p1.open();
        UpdateScan src2 = (UpdateScan) p2.open();
        TempTable result = new TempTable(sch, tx);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        int value = 0;
        while (hasmore1 && hasmore2) {
            value = comp.compare(src1, src2);
//            System.out.println("1st run: "
//                    + src1.getVal("Sname")
//                    + " vs 2nd run: "
//                    + src2.getVal("Sname")
//                    + "Value "
//                    + value);
            if (value != 0) {
                if (value < 0) {
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore2 = copy(src2, dest);
                }
            } else {
//                System.out.println("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP "
//                        + "Src1: " + src1.getString("Sname") + " Src2: " + src2.getString("Sname"));
                hasmore1 = src1.next();
                src2.delete();

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

    protected boolean copy(Scan scan, UpdateScan newUpdateScanObject) {
        newUpdateScanObject.insert();
        for (String fldname : sch.fields()) {
            newUpdateScanObject.setVal(fldname, scan.getVal(fldname));
        }
        return scan.next();
    }
}
