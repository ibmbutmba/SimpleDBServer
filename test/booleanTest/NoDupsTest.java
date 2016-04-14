package booleanTest;

import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.query.TableScan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author mady
 */
public class NoDupsTest {

    static Transaction tx;
    static final String tableName = "STUDENTTEST";
    static final String dbName = "mytestdb";
    private TableInfo ti;
    private TableScan ts;
    private static Schema schema;

    public NoDupsTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        SimpleDB.init(dbName);
        tx = new Transaction();

        schema = new Schema();
        schema.addIntField("SId");
        schema.addStringField("Sname", 20);
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Boyko");

        file.setInt("SId", 1);
        file.setString("Sname", "Boyko");

        file.setInt("SId", 5);
        file.setString("Sname", "Dani");
        tx.commit();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
    }

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

        assertEquals(5, ts.getInt("SId"));
        assertEquals("Dani", ts.getString("Sname"));
    }
}
