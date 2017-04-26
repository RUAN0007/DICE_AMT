import java.sql.*;
import java.util.*;

public class SQLiteJDBC {

  private Connection dbCon = null;

  public SQLiteJDBC(String dbPath){
    try{
      Class.forName("org.sqlite.JDBC");
      dbCon = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
      System.out.println("Opened database successfully");
    }catch(Exception e) {

      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      System.err.println("Open Sqlite DB Failed...");
    }
  }

  public boolean diceExec(String sql) {
    try{
      Statement stmt = dbCon.createStatement();
      stmt.executeUpdate(sql);
      stmt.close(); 
    }catch(Exception e) {
      System.err.println("Fail to execute SQL: " + sql);
      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      return false;
    }
    return true;
  }

  /*
  Execute sql select query on Sqlite

  Args:
    sql: the sql to execute

  Returns:
    a Map of query results:
      key: attribute/field name
      value: a list of attribute/field value satisifying the predicates
  */
  public Map<String, List<String>> diceSelect(String sql) {
    try {

      Map<String, List<String>> result = new HashMap<>();
      Statement stmt = dbCon.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      ResultSetMetaData rsmd = rs.getMetaData();
      int columnCount = rsmd.getColumnCount();

      // The column count starts from 1
      for (int i = 1; i <= columnCount; i++ ) {
        String name = rsmd.getColumnName(i);
        result.put(name, new ArrayList<String>());//Init each attribute with an empty list
      }
      while(rs.next()) {
        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
          String attrName = entry.getKey();
          List<String> attrValues = entry.getValue();

          String attrValue = rs.getString(attrName);
          attrValues.add(attrValue);
          result.put(attrName, attrValues);
        }
      }
      return result;
    }catch(Exception e) {
      System.err.println("Fail to execute SQL: " + sql);
      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
      return null;
    }

  }

}