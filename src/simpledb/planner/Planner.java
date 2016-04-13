package simpledb.planner;

import simpledb.tx.Transaction;
import simpledb.parse.*;
import simpledb.query.*;

/**
 * The object that executes SQL statements.
 *
 * @author sciore
 */
public class Planner {

    private QueryPlanner qplanner;
    private UpdatePlanner uplanner;

    public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
        this.qplanner = qplanner;
        this.uplanner = uplanner;
    }

    /**
     * Creates a plan for an SQL select statement, using the supplied planner.
     *
     * @param qry the SQL query string
     * @param tx the transaction
     * @return the scan corresponding to the query plan
     */
    public Plan createQueryPlan(String qry, Transaction tx) {
        Parser parser = new Parser(qry);
        QueryData data = parser.query();
        System.out.println("Planner createQueryPlan " + data);
        return qplanner.createPlan(data, tx);
    }

    /**
     * Executes an SQL insert, delete, modify, or create statement. The method
     * dispatches to the appropriate method of the supplied update planner,
     * depending on what the parser returns.
     *
     * @param cmd the SQL update string
     * @param tx the transaction
     * @return an integer denoting the number of affected records
     */
    public int executeUpdate(String cmd, Transaction tx) {
        System.out.println("planner.Planner executeUpdate triggered - String : " + cmd);
        String convertedFromBooleanToString =  convertBoolean(cmd);
        Parser parser = new Parser(convertedFromBooleanToString);
        Object obj = parser.updateCmd();
        if (obj instanceof InsertData) {
            return uplanner.executeInsert((InsertData) obj, tx);
        } else if (obj instanceof DeleteData) {
            return uplanner.executeDelete((DeleteData) obj, tx);
        } else if (obj instanceof ModifyData) {
            return uplanner.executeModify((ModifyData) obj, tx);
        } else if (obj instanceof CreateTableData) {
            return uplanner.executeCreateTable((CreateTableData) obj, tx);
        } else if (obj instanceof CreateViewData) {
            return uplanner.executeCreateView((CreateViewData) obj, tx);
        } else if (obj instanceof CreateIndexData) {
            return uplanner.executeCreateIndex((CreateIndexData) obj, tx);
        } else {
            return 0;
        }
    }
    
    private String convertBoolean(String query){
         //int indexOfTrue = query.indexOf("true");
         String replacedValue = query.replace("true", "1");
         replacedValue = replacedValue.replace("false", "0");
         System.out.println("private convertBoolean "  + replacedValue);
         return replacedValue;
    }
}
