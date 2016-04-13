package booleanTest;


import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.metadata.TableMgr;
import simpledb.query.TableScan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author artur
 */
public class MyTest {

    static Transaction tx;
    static final String tableName = "STUDENTTEST";
    static final String dbName = "mytestdb";
    private TableInfo ti;
    private TableScan ts;
    private static Schema schema;

    public MyTest() {
    }

    @BeforeClass
    public static void setUpClass() {

        SimpleDB.init(dbName);
        tx = new Transaction();

        schema = new Schema();
        schema.addIntField("SId");
        schema.addStringField("Sname", 20);
        schema.addBooleanField("Sbool");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Boyko");
        file.setBoolean("Sbool", true);

        tx.commit();
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
        tx.commit();
    }

    @Test
    public void testGetDataBack() {
        ti = new TableInfo(tableName, schema);
        ts = new TableScan(ti, tx);
        ts.beforeFirst();
        ts.next();
        
        System.out.println("blblbllbl " + ts.getInt("SId"));
        assertEquals(ts.getInt("SId"), 1);
    }
    
    @Test
    public void testGetBooleanBack() {
        ti = new TableInfo(tableName, schema);
        ts = new TableScan(ti, tx);
        ts.beforeFirst();
        ts.next();
        
        assertEquals(ts.getBoolean("Sbool"), true);
    }

}
