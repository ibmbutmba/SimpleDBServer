/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

/**
 *
 * @author mady
 */
public class NoDupsHashPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private List<String> fields;

    public NoDupsHashPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
        fields = sortfields;
    }

    @Override
    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRunsHashing(src);//it can be changed to splitIntoRuns(src)
        src.close();
        System.out.println("Runs size: " + runs.size());
        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new NoDupsHashScan(runs, comp);
    }

    @Override
    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRunsHashing(Scan scan1) {
        UpdateScan scan = (UpdateScan) scan1;
        List<TempTable> temps = new ArrayList<TempTable>();
        TempTable temp1 = new TempTable(sch, tx);
        TempTable temp2 = new TempTable(sch, tx);
        scan.beforeFirst();
        if (!scan.next()) {
            return temps;
        }
        temps.add(temp1);
        temps.add(temp2);

        int value = 0;
        int i = 0;
        ArrayList<Constant> temp1Values = new ArrayList();
        ArrayList<Constant> temp2Values = new ArrayList();

        HashMap<String, Integer> hmap = new HashMap<String, Integer>();
        hmap.put("a", 1);
        hmap.put("b", 2);
        hmap.put("c", 3);
        hmap.put("d", 4);
        hmap.put("e", 5);
        hmap.put("f", 6);
        hmap.put("g", 7);
        hmap.put("h", 8);
        hmap.put("i", 9);
        hmap.put("j", 10);
        hmap.put("k", 11);
        hmap.put("l", 12);
        hmap.put("m", 13);
        hmap.put("n", 14);
        hmap.put("o", 15);
        hmap.put("p", 16);
        hmap.put("q", 17);
        hmap.put("r", 18);
        hmap.put("s", 19);
        hmap.put("t", 20);
        hmap.put("u", 21);
        hmap.put("v", 22);
        hmap.put("w", 23);
        hmap.put("x", 24);
        hmap.put("y", 25);
        hmap.put("z", 26);

        //calculates the distance between each of the first 5 letters and "m"
        //calculates the sum of these values
        //uses this sum for splitting the values in 2 runs
        while (i < 5 && scan.getString("Sname").length() > i) {
            value += hmap.get(("" + (scan.getString("Sname").charAt(i))).toLowerCase());
            i++;
            if (i == 5) {
                addtoTemp(value, scan, temp1Values, temp2Values);
                i = 0;
                value = 0;
                if (!scan.next()) {
                    break;
                }
            }
        }

        setValues(temp1, temp1Values);
        setValues(temp2, temp2Values);

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
            if (value != 0) {
                if (value < 0) {
                    hasmore1 = copy(src1, dest);
                } else {
                    hasmore2 = copy(src2, dest);
                }
            } else {
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

    
     //the minimum sum is 0 and the maximum is 130; we make 2 runs that have approximately the same size by comparing the value with 65
    private void addtoTemp(int value, UpdateScan scan, ArrayList<Constant> temp1Values, ArrayList<Constant> temp2Values) {
        boolean existing = false;
        if (value < 65) {
            for (Constant cons : temp1Values) {
                if (scan.getVal("Sname").equals(cons)) {
                    existing = true;
                }
            }
            if (!existing) {
                temp1Values.add(scan.getVal("Sname"));
            }
            existing = false;
        } else {
            for (Constant cons : temp2Values) {
                if (scan.getVal("Sname").equals(cons)) {
                    existing = true;
                }
            }
            if (!existing) {
                temp2Values.add(scan.getVal("Sname"));
            }
            existing = false;
        }
    }

    private void setValues(TempTable temp1, ArrayList<Constant> namesList) {
        UpdateScan updateScan1 = temp1.open();
        for (Constant actualScan : namesList) {
            updateScan1.insert();
//            for (String fldname : sch.fields()) {
            updateScan1.setVal("Sname", actualScan);
//            }
        }
        updateScan1.close();
    }

}