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

public class NewDataTypesTest {

    static Transaction tx;
    static final String tableName = "STUDENTTEST";
    static final String dbName = "mytestdb";
    private TableInfo ti;
    private TableScan ts;
    private static Schema schema;

    @BeforeClass
    public static void setUpClass() {

        SimpleDB.init(dbName);
        tx = new Transaction();

        schema = new Schema();
        schema.addIntField("SId");
        schema.addStringField("Sname", 20);
        schema.addBooleanField("Sbool");
        schema.addFloatField("gradeAverage");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setInt("SId", 1);
        file.setString("Sname", "Boyko");
        file.setBoolean("Sbool", true);
        file.setFloat("gradeAverage", 67.94f);

        tx.commit();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
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

    @Test
    public void testFloatGetDataBack() {
        ti = new TableInfo(tableName, schema);
        ts = new TableScan(ti, tx);
        ts.beforeFirst();
        ts.next();

        assertEquals(67.94f, ts.getFloat("gradeAverage"), 0.0001);
    }

}
