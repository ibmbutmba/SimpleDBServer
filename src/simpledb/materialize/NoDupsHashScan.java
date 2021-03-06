/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.materialize;

import java.util.Arrays;
import java.util.List;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;

/**
 *
 * @author mady
 */
public class NoDupsHashScan implements Scan {

    private List<String> indices;
    private GroupValue groupval;
    private boolean moregroups;
    private RecordComparator comp;
    private boolean hasmore1, hasmore2 = false;
    private UpdateScan s1, s2 = null, currentscan = null;
    private List<RID> savedposition;

    public NoDupsHashScan( List<TempTable> runs, RecordComparator comp ) {

        this.indices = indices;
        this.comp = comp;
        s1 = ( UpdateScan ) runs.get( 0 ).open();
        hasmore1 = s1.next();
        if ( runs.size() > 1 ) {
            s2 = ( UpdateScan ) runs.get( 1 ).open();
            hasmore2 = s2.next();
        }
    }

    public void beforeFirst() {
        currentscan = null;
        s1.beforeFirst();
        hasmore1 = s1.next();
        if ( s2 != null ) {
            s2.beforeFirst();
            hasmore2 = s2.next();
        }
    }

    public boolean next() {
        if ( currentscan != null ) {
            if ( currentscan == s1 ) {
                hasmore1 = s1.next();
            } else if ( currentscan == s2 ) {
                hasmore2 = s2.next();
            }
        }
        if ( !hasmore1 && !hasmore2 ) {
            return false;
        } else if ( hasmore1 && hasmore2 ) {
//            System.out.println("In the no dups sort scan "+ s1.getVal("Sname")+ " "+ s2.getVal("Sname"));
            if ( comp.compare( s1, s2 ) < 0 ) {
                currentscan = s1;
//            } else if (comp.compare(s1, s2) == 0) {
////                System.out.println("Here we have equality: " + " "
////                        + s1.getInt("SId") + " "
////                        + s1.getString("Sname") + " "
////                        + s2.getInt("SId") + " "
////                        + s2.getString("Sname"));
//                s1.next();
//                currentscan = s2;
////                removeDuplicate(s2);
            } else {
                //Part of head
                hasmore1= s1.next();
                //Dev2
//                System.out.println("S1 >= s2");
//                hasmore1= s1.next(); ////////to add this lin for the case when s1==s2

                currentscan = s2;
            }
        } else if ( hasmore1 ) {
            currentscan = s1;
        } else if ( hasmore2 ) {
            currentscan = s2;
        }
        return true;
    }

    public void close() {
        s1.close();
        if ( s2 != null ) {
            s2.close();
        }
    }

    @Override
    public Constant getVal( String fldname ) {
        return currentscan.getVal( fldname );
    }

    @Override
    public int getInt( String fldname ) {
        return ( Integer ) getVal( fldname ).asJavaVal();
    }

    @Override
    public String getString( String fldname ) {
        return ( String ) getVal( fldname ).asJavaVal();
    }

    @Override
    public boolean getBoolean( String fldname ) {
        return ( Boolean ) getVal( fldname ).asJavaVal();
    }

    @Override
    public boolean hasField( String fldname ) {
        if ( indices.contains( fldname ) ) {
            return true;
        }
        return false;
    }

    public void savePosition() {
        RID rid1 = s1.getRid();
        RID rid2 = (s2 == null) ? null : s2.getRid();
        savedposition = Arrays.asList( rid1, rid2 );
    }

    public void restorePosition() {
        RID rid1 = savedposition.get( 0 );
        RID rid2 = savedposition.get( 1 );
        s1.moveToRid( rid1 );
        if ( rid2 != null ) {
            s2.moveToRid( rid2 );
        }
    }

    public void removeDuplicate( UpdateScan uscan ) {
        uscan.delete();
    }

}