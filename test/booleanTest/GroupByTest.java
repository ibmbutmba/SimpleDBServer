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
import simpledb.materialize.GroupByPlan;
import simpledb.materialize.GroupByScan;
import simpledb.query.Plan;
import simpledb.query.Predicate;
import simpledb.query.SelectPlan;
import simpledb.query.TablePlan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class GroupByTest {

    //Init db
    static final String dbName = "mytestdb";

    //Creation of table
    static Transaction transaction;
    static final String tableName = "STUDENTTEST";
    private static Schema schema;

    //Group by plan components
    private static GroupByPlan groupByPlan;
    private static SelectPlan selectPlan;
    private static Plan tablePlan;

    //Select plan components
    private static Predicate predicate = new Predicate();
    ;
    private static List<AggregationFn> aggregationFnList = new ArrayList();

    //Sort (filter) elems in table based on curent fields
    private static List<String> filterOn = Arrays.asList("Sname", "SId");
    private static GroupByScan groupByScan;

    //Sorted values (for testing)
    private static List<String> sortedNames = Arrays.asList("Artur", "Boyko", "Boyko", "Cada", "Mada", "Vada");

    @BeforeClass
    public static void setUpClass() {
        //Init connection with db
        SimpleDB.init(dbName);

        //Create a transaction (the activity to do sth)
        transaction = new Transaction();

        //Definition of the table structure
        schema = new Schema();
        schema.addIntField("SId");
        schema.addStringField("Sname", 20);

        //Create a table
        SimpleDB.mdMgr().createTable(tableName, schema, transaction);

        //Get the table from the manager
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, transaction);
        //Manages a file of records
        RecordFile file = new RecordFile(tableInfo, transaction);

        //Components of GroupByPlan 
        tablePlan = new TablePlan(tableName, transaction);
        selectPlan = new SelectPlan(tablePlan, predicate);

        //Init group by plan
        groupByPlan = new GroupByPlan(selectPlan, filterOn, aggregationFnList, transaction);

        file.insert();
        file.setInt("SId", 4);
        file.setString("Sname", "Boyko");

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Boyko");

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Artur");

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Mada");

        file.insert();
        file.setInt("SId", 5);
        file.setString("Sname", "Cada");

        file.insert();
        file.setInt("SId", 2);
        file.setString("Sname", "Vada");

        //commit
        transaction.commit();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        transaction.commit();
    }

    @Test
    public void testGroupBy() {
        //Open
        groupByScan = (GroupByScan) groupByPlan.open();

        //Go to 1st
        groupByScan.beforeFirst();

        //Loop through group by scan results
        int loopId = 0;
        while (groupByScan.next()) {
            System.out.println("next " + groupByScan.getVal("SId") + " " + groupByScan.getVal("Sname"));

            assertEquals(sortedNames.get(loopId), groupByScan.getString("Sname"));

            loopId++;
        }

        System.out.println("Distinct values: " + groupByPlan.distinctValues("SId"));
    }
}
