package booleanTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.materialize.AggregationFn;
import simpledb.materialize.NoDupsHashPlan;
import simpledb.materialize.NoDupsHashScan;
import simpledb.materialize.NoDupsSortPlan;
import simpledb.materialize.NoDupsSortScan;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class NoDupsTest {

    //Init db
    static final String dbName = "mytestdb";

    //Creation of table
    static Transaction transaction;
    static final String tableName = "STUDENTTEST";
    private static Schema schema;

    //Planner componenets
    private static NoDupsSortPlan noDupsSortPlan;
    private static NoDupsHashPlan noDupsHashPlan;
    private static SelectPlan selectPlan;
    private static Plan tablePlan;

    //Select plan components
    private static Predicate predicate = new Predicate();
    ;
    private static List<AggregationFn> aggregationFnList = new ArrayList();

    //Sort (filter) elems in table based on curent fields
    private static String distinctFilter = "Sname";
    private static List<String> filterOn = Arrays.asList( "Sname", "Genre" );

    //Actual scanners
    private static NoDupsSortScan noDupsSortScan;
    private static NoDupsHashScan noDupsHashScan;

    //Sorted values (for testing)S
    private static List<String> sortedNames = Arrays.asList( "AAAAA", "BBBBB", "CCCCC", "DDDDD", "EEEEE", "FFFFF", "GGGGG", "MamboNumba" );

    @BeforeClass
    public static void setUpClass() {
        //Init connection with db
        SimpleDB.init( dbName );

        //Create a transaction (the activity to do sth)
        transaction = new Transaction();

        //Definition of the table structure
        schema = new Schema();
        schema.addIntField( "SId" );
        schema.addStringField( "Genre", 10 );
        schema.addStringField( "Sname", 20 );

        //Create a table
        SimpleDB.mdMgr().createTable( tableName, schema, transaction );

        //Get the table from the manager
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo( tableName, transaction );
        //Manages a file of records
        RecordFile file = new RecordFile( tableInfo, transaction );

        //Components of GroupByPlan 
        tablePlan = new TablePlan( tableName, transaction );
        selectPlan = new SelectPlan( tablePlan, predicate );

        //Init group by plan
        noDupsSortPlan = new NoDupsSortPlan( selectPlan, filterOn, transaction );
        noDupsHashPlan = new NoDupsHashPlan( selectPlan, filterOn, transaction );

        file.insert();
        file.setInt( "SId", 1 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "AAAAA" );

        file.insert();
        file.setInt( "SId", 2 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "BBBBB" );

        file.insert();
        file.setInt( "SId", 3 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "CCCCC" );

        file.insert();
        file.setInt( "SId", 4 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "DDDDD" );

        file.insert();
        file.setInt( "SId", 5 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "EEEEE" );

        file.insert();
        file.setInt( "SId", 6 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "CCCCC" );

        file.insert();
        file.setInt( "SId", 7 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "BBBBB" );

        file.insert();
        file.setInt( "SId", 8 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "AAAAA" );

        file.insert();
        file.setInt( "SId", 9 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "FFFFF" );

        file.insert();
        file.setInt( "SId", 10 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "GGGGG" );

        file.insert();
        file.setInt( "SId", 11 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "BBBBB" );

        file.insert();
        file.setInt( "SId", 9 );
        file.setString( "Genre", "M" );
        file.setString( "Sname", "FFFFF" );

        file.insert();
        file.setInt( "SId", 9 );
        file.setString( "Genre", "F" );
        file.setString( "Sname", "MamboNumba" );

        //commit
        transaction.commit();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase( dbName );
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        transaction.commit();
    }

    @Test
    public void testSort() {
        //Open
        noDupsSortScan = ( NoDupsSortScan ) noDupsSortPlan.open();

        //Go to 1st
        noDupsSortScan.beforeFirst();

        //Loop through group by scan results
        int loopId = 0;
        while ( noDupsSortScan.next() ) {
            System.out.println( "Comparing DB : '" + noDupsSortScan.getVal( "Sname" ) + "' vs Predictions : '" + sortedNames.get( loopId ) + "'" );

            assertEquals( sortedNames.get( loopId ), noDupsSortScan.getString( "Sname" ) );
            loopId++;
        }
    }

    @Test
    public void testHash() {
        //Open
        noDupsHashScan = ( NoDupsHashScan ) noDupsHashPlan.open();

        //Go to 1st
        noDupsHashScan.beforeFirst();

        //Loop through group by scan results
        int loopId = 0;
        while ( noDupsHashScan.next() ) {
            System.out.println( "Comparing DB : '" + noDupsHashScan.getVal( "Sname" ) + "' vs Predictions : '" + sortedNames.get( loopId ) + "'" );
            assertEquals( sortedNames.get( loopId ), noDupsHashScan.getString( "Sname" ) );
            loopId++;
        }
    }
}
