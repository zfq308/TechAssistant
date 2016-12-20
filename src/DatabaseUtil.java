/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

/**
 *
 * @author Raviraj
 */
//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
public class DatabaseUtil {

    public String vendor;
    String host;
    String port;
    public String dbname;
    String user;
    String password;
    static long start, stop;
    String version;
//--------------------------------------------------------------------------------

    public DatabaseUtil() {
    }

//--------------------------------------------------------------------------------
    public DatabaseUtil(String filePath) {
        File file = new File(filePath);
        setDbInfo(file);
    }

//--------------------------------------------------------------------------------
    public DatabaseUtil(String vendor, String host, String port, String dbname, String user, String password) {
        this.vendor = vendor;
        this.host = host;
        this.port = port;
        this.dbname = dbname;
        this.user = user;
        this.password = password;
    }

//--------------------------------------------------------------------------------
    public void setDbInfo(String vendor, String host, String port, String dbname, String user, String password) {
        this.vendor = vendor;
        this.host = host;
        this.port = port;
        this.dbname = dbname;
        this.user = user;
        this.password = password;
    }

//--------------------------------------------------------------------------------
    public void setDbInfo(File file) {
        try {
            Scanner read = new Scanner(file);
            while (read.hasNext()) {
                String head = "", value = "";
                String tmp = read.nextLine();
                StringTokenizer token = new StringTokenizer(tmp, "=");
                while (token.hasMoreTokens()) {
                    head = token.nextToken();
                    value = token.nextToken();
                }
                if (head.equals("vendor")) {
                    this.vendor = value;
                }
                if (head.equals("host")) {
                    this.host = value;
                }
                if (head.equals("port")) {
                    this.port = value;
                }
                if (head.equals("dbname")) {
                    this.dbname = value;
                }
                if (head.equals("user")) {
                    this.user = value;
                }
                if (head.equals("password")) {
                    this.password = value;
                }
            }
        } catch (FileNotFoundException fnfe) {
            System.out.println("File " + file.getName() + " doesnot exist!");
            System.out.println("contents of the file is as below:");
            System.out.println("================================");
            System.out.println("vendor=mysql|mssql");
            System.out.println("host=localhost");
            System.out.println("port=3306");
            System.out.println("dbname=test");
            System.out.println("user=username");
            System.out.println("password=secret");
        }
    }

//--------------------------------------------------------------------------------
    public String showDbInfo() {
        return (host + ":" + port + "\\" + dbname + "|user=" + user);
    }

//--------------------------------------------------------------------------------
    public void setMySQLSession(Statement stmt) {
        if (vendor.equalsIgnoreCase("mysql")) {
            try {
                //increasing wait_timeout for the session to make it infinite
                stmt.executeUpdate("SET SESSION WAIT_TIMEOUT=9999999999");
                Logs.write("Raising mysql WAIT_TIMEOUT");
                //disabling the query result cache for the session
                stmt.executeUpdate("SET SESSION QUERY_CACHE_TYPE=0");
                Logs.write("Disabling mysql QUERY_CACHE+_TYPE");
            } catch (SQLException sqle) {
                Logs.write("error while setting timeout", sqle);
            }
        }
    }

//--------------------------------------------------------------------------------
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        Connection con = null;
        if (this.vendor.equalsIgnoreCase("mysql")) {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbname, user, password);
        } else if (this.vendor.equalsIgnoreCase("mssql")) {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection("jdbc:sqlserver://" + host + ":" + port + ";"
                    + "databaseName=" + dbname + ";user=" + user + ";password=" + password + ";");
        } else {
            JOptionPane.showMessageDialog(null, "Unknown Vendor");
        }

        return con;
    }

//--------------------------------------------------------------------------------
    public boolean testConnection() {
        boolean test = false;
        try {
            Connection con = null;
            if (this.vendor.equalsIgnoreCase("mysql")) {
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + dbname, user, password);
                test = true;
            } else if (this.vendor.equalsIgnoreCase("mssql")) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                con = DriverManager.getConnection("jdbc:sqlserver://" + host + ":" + port + ";"
                        + "databaseName=" + dbname + ";user=" + user + ";password=" + password + ";");
                test = true;
            } else {
                test = false;
            }

            if (test == true) {
                setDBVendorDetails(con);				
				JOptionPane.showMessageDialog(null,"Connection was Successful!\n" + version);
            }
            con.close();
        } catch (ClassNotFoundException cnfe) {
            JOptionPane.showMessageDialog(null, cnfe.getMessage(), "Unable to locate db connection libraries", JOptionPane.ERROR_MESSAGE);
            Logs.write("testConnection()", cnfe);
        } catch (SQLException sqle) {
            JOptionPane.showMessageDialog(null, sqle.getMessage(), "Connection Failed", JOptionPane.ERROR_MESSAGE);
            Logs.write("testConnection", sqle);
        }

        return test;
    }

//--------------------------------------------------------------------------------
	public void setDBVendorDetails(Connection con) throws SQLException
	{
		DatabaseMetaData dmtd = con.getMetaData();
		version = dmtd.getDatabaseProductName() +" " + dmtd.getDatabaseProductVersion() + "." + dmtd.getDatabaseMajorVersion() + "." + dmtd.getDatabaseMinorVersion();
		
	}
	
//--------------------------------------------------------------------------------
    public Vector<String> getTablesInVector(Connection con) throws SQLException {
        Vector<String> tableList = new Vector<String>();
        DatabaseMetaData mtd = con.getMetaData();
        String type[] = {"TABLE"};
        ResultSet rs = mtd.getTables(null, null, "%", type);
        while (rs.next()) {
            tableList.add(rs.getString("TABLE_NAME"));
        }
        return tableList;
    }

//--------------------------------------------------------------------------------
    public Vector<String> getColumnsForTableInVector(Connection con, String table) throws SQLException {
        Vector<String> columnList = new Vector<String>();
        DatabaseMetaData mtd = con.getMetaData();
        ResultSet rs = mtd.getColumns(null, null, table, "%");
        while (rs.next()) {
            columnList.add(rs.getString("COLUMN_NAME"));
        }
        return columnList;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getRowCountForTablesInVector(Connection con) throws SQLException {
        Vector<Object> tableRowCounts = new Vector<Object>();
        Vector<String> tables = getTablesInVector(con);
        Statement stmt = con.createStatement();

        Enumeration enumerate = tables.elements();
        while (enumerate.hasMoreElements()) {
            String tmp = (String) enumerate.nextElement();
            String query = "";
            if (vendor.equalsIgnoreCase("mysql")) {
                query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (vendor.equalsIgnoreCase("mssql")) {
                query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }

            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            Vector<String> rowCounts = new Vector<String>();
            rowCounts.add(new String(tmp));
            rowCounts.add(new String(rs.getString(1)));
            tableRowCounts.add(rowCounts);
        }

        return tableRowCounts;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getRowCountComparisonsForTwoDatabasesInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2) throws SQLException {
        Vector<Object> tableRowCountCompare = new Vector<Object>(2000, 10);
        String table2RowCount = "";

        Vector<String> tables = getTablesInVector(con1);

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        System.out.println("Statement 1 Query timeout: " + stmt1.getQueryTimeout());
        System.out.println("Statement 1 Query timeout: " + stmt1.getQueryTimeout());

        Enumeration enumerate = tables.elements();
        String tmp = "";
        String dbUtil1Query = "";
        String dbUtil2Query = "";
        //Vector<String> rowCountCompare = new Vector<String>(); //put here when used clear()
        while (enumerate.hasMoreElements()) {
            tmp = (String) enumerate.nextElement();
            dbUtil1Query = "";
            dbUtil2Query = "";
            if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }


            Vector<String> rowCountCompare = new Vector<String>(); //-- replaced with clear()
            //rowCountCompare.clear();
            rowCountCompare.add(new String(tmp));

            ResultSet rs1 = stmt1.executeQuery(dbUtil1Query);
            rs1.next();
            rowCountCompare.add(new String(rs1.getString(1)));

            try {
                ResultSet rs2 = stmt2.executeQuery(dbUtil2Query);
                rs2.next();
                table2RowCount = rs2.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table2RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table2RowCount = "Not Found";
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    Logs.write("getRowCountComparisonsForTwoDatabasesInVector()", mysqlsee);
                }
            }

            rowCountCompare.add(new String(table2RowCount));

            if (rs1.getString(1).equals(table2RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }
            rowCountCompare.add(new String(tmp));
            tableRowCountCompare.add(rowCountCompare);
        }


        return tableRowCountCompare;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getRowCountComparisonsForTwoDatabasesInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2, JProgressBar bar1, JProgressBar bar2) throws ClassNotFoundException, SQLException {
        Vector<Object> tableRowCountCompare = new Vector<Object>();
        String table2RowCount = "";

        Vector<String> tables = getTablesInVector(con1);

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        setMySQLSession(stmt1);
        setMySQLSession(stmt2);

        Enumeration enumerate = tables.elements();
        String tmp = "";
        String dbUtil1Query = "";
        String dbUtil2Query = "";
        //Vector<String> rowCountCompare = new Vector<String>(); //put here when used clear()
        int i = 0;
        int limit = tables.size();

        ResultSet rs1 = null;
        ResultSet rs2 = null;

        while (enumerate.hasMoreElements()) {
            tmp = (String) enumerate.nextElement();
            dbUtil1Query = "";
            dbUtil2Query = "";
            if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }

            Vector<String> rowCountCompare = new Vector<String>(); //-- replaced with clear()
            //rowCountCompare.clear();
            rowCountCompare.add(new String(tmp));


            try {
                rs1 = stmt1.executeQuery(dbUtil1Query);
                rs1.next();
                rowCountCompare.add(new String(rs1.getString(1)));
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Communications link failure") != -1) {
                    con1 = dbUtil1.getConnection();
                    rowCountCompare.add(new String("failed"));
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    DBInfo.resetProgressBars();
                    Logs.write("getRowCountComparisonsForTwoDatabasesInVector - primary db, while retrieving count for table " + tmp, mysqlsee);
                }

            }

            try {
                rs2 = stmt2.executeQuery(dbUtil2Query);
                rs2.next();
                table2RowCount = rs2.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table2RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table2RowCount = "Not Found";
                } else {
                    if (mysqlsee.getMessage().indexOf("Communications link failure") != -1) {
                        con2 = dbUtil2.getConnection();
                        rowCountCompare.add(new String("failed"));
                    } else {
                        JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                        DBInfo.resetProgressBars();
                        Logs.write("getRowCountComparisonsForTwoDatabasesInVector - secondary db, while retrieving count for table " + tmp, mysqlsee);
                    }
                }
            }

            rowCountCompare.add(new String(table2RowCount));

            if (rs1.getString(1).equals(table2RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }
            rowCountCompare.add(new String(tmp));
            tableRowCountCompare.add(rowCountCompare);
            i++;
            DBInfo.updateProgressStatus(bar1, i, limit);
            DBInfo.updateProgressStatus(bar2, i, limit);
        }
        DBInfo.resetProgressBars();
        return tableRowCountCompare;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getRowCountComparisonsForTwoDatabasesInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2, boolean ignoreTablesSelected, JProgressBar bar1, JProgressBar bar2) throws ClassNotFoundException, SQLException {
        Vector<Object> tableRowCountCompare = new Vector<Object>();
        String table2RowCount = "";

        Vector<String> tables = getTablesInVector(con1);
        Vector<String> specifiedTables = VectorUtil.getListOfIgnoreTables("specified_tables.csv");
        if(specifiedTables.size() != 0)
        {            
            if(ignoreTablesSelected == true)
            {
                Logs.write("Ignoring specified tables only in Primary DB 1 for compare");
                tables.removeAll(specifiedTables);
            }
            else
            {
                Logs.write("Considering specified tables only in Primary DB 1 for compare");
                tables = specifiedTables;
            }
        }
        else
        {
            Logs.write("Considering all the tables in Primary DB 1 for compare");
        }

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        setMySQLSession(stmt1);
        setMySQLSession(stmt2);

        Enumeration enumerate = tables.elements();
        String tmp = "";
        String dbUtil1Query = "";
        String dbUtil2Query = "";
        //Vector<String> rowCountCompare = new Vector<String>(); //put here when used clear()
        int i = 0;
        int limit = tables.size();

        ResultSet rs1 = null;
        ResultSet rs2 = null;

        while (enumerate.hasMoreElements()) {
            tmp = (String) enumerate.nextElement();
            dbUtil1Query = "";
            dbUtil2Query = "";
            if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }

            Vector<String> rowCountCompare = new Vector<String>(); //-- replaced with clear()
            //rowCountCompare.clear();
            rowCountCompare.add(new String(tmp));


            try {
                rs1 = stmt1.executeQuery(dbUtil1Query);
                rs1.next();
                rowCountCompare.add(new String(rs1.getString(1)));
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Communications link failure") != -1) {
                    con1 = dbUtil1.getConnection();
                    rowCountCompare.add(new String("failed"));
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    DBInfo.resetProgressBars();
                    Logs.write("getRowCountComparisonsForTwoDatabasesInVector - primary db, while retrieving count for table " + tmp, mysqlsee);
                }

            }

            try {
                rs2 = stmt2.executeQuery(dbUtil2Query);
                rs2.next();
                table2RowCount = rs2.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table2RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table2RowCount = "Not Found";
                } else {
                    if (mysqlsee.getMessage().indexOf("Communications link failure") != -1) {
                        con2 = dbUtil2.getConnection();
                        rowCountCompare.add(new String("failed"));
                    } else {
                        JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                        DBInfo.resetProgressBars();
                        Logs.write("getRowCountComparisonsForTwoDatabasesInVector - secondary db, while retrieving count for table " + tmp, mysqlsee);
                    }
                }
            }

            rowCountCompare.add(new String(table2RowCount));

            if (rs1.getString(1).equals(table2RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }
            rowCountCompare.add(new String(tmp));
            tableRowCountCompare.add(rowCountCompare);
            i++;
            DBInfo.updateProgressStatus(bar1, i, limit);
            DBInfo.updateProgressStatus(bar2, i, limit);
        }
        DBInfo.resetProgressBars();
        return tableRowCountCompare;
    }

    //NEWLY DEFINED--------------------------------------------------------------------------------
    public static String[] getTablesInArray(Connection con) throws SQLException {
        String tables[] = new String[2500];
        DatabaseMetaData mtd = con.getMetaData();
        String type[] = {"TABLE"};
        ResultSet rs = mtd.getTables(null, null, "%", type);
        int i = 0;
        while (rs.next()) {
            tables[i] = rs.getString("TABLE_NAME");
            i++;
        }

        return tables;
    }

    //NEWLY DEFINED--------------------------------------------------------------------------------
    public static String[][] getRowCountForTablesInArray(Connection con, DatabaseUtil dbUtil) {
        String tableWithRowCounts[][] = new String[30][2];
        try {
            String tables[] = getTablesInArray(con);
            Statement stmt = con.createStatement();
            ResultSet rs;
            String dbUtil1Query = "";
            for (int i = 0; i < tables.length; i++) {
                if (tables[i] == null) {
                    break;
                }

                tableWithRowCounts[i][0] = tables[i];
                if (dbUtil.getVendor().equalsIgnoreCase("mysql")) {
                    dbUtil1Query = "SELECT COUNT(*) FROM `" + tables[i] + "`";
                } else if (dbUtil.getVendor().equalsIgnoreCase("mssql")) {
                    dbUtil1Query = "SELECT COUNT(*) FROM [" + tables[i] + "]";
                }
                rs = stmt.executeQuery(dbUtil1Query);
                rs.next();
                tableWithRowCounts[i][1] = rs.getString(1);
            }
        } catch (SQLException sqle) {
            Logs.write("getRowCountForTablesInArray", sqle);
        }

        return tableWithRowCounts;
    }

    //--------------------------------------------------------------------------------
    public Vector<Object> getRowCountComparisonsForTwoDatabasesInVector2(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2) throws SQLException {
        Vector<Object> tableRowCountCompare = new Vector<Object>(1000);
        String table2RowCount = "";

        Vector<String> tables = getTablesInVector(con1);

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        Enumeration enumerate = tables.elements();
        String tmp = "";
        String dbUtil1Query = "";
        String dbUtil2Query = "";
        //Vector<String> rowCountCompare = new Vector<String>(); //put here when used clear()
        while (enumerate.hasMoreElements()) {
            tmp = (String) enumerate.nextElement();
            dbUtil1Query = "";
            dbUtil2Query = "";
            if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }


            Vector<String> rowCountCompare = new Vector<String>(); //-- replaced with clear()
            //rowCountCompare.clear();
            rowCountCompare.add(new String(tmp));

            ResultSet rs1 = stmt1.executeQuery(dbUtil1Query);
            rs1.next();
            rowCountCompare.add(new String(rs1.getString(1)));

            try {
                ResultSet rs2 = stmt2.executeQuery(dbUtil2Query);
                rs2.next();
                table2RowCount = rs2.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table2RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table2RowCount = "Not Found";
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    Logs.write("getRowCountComparisonsForTwoDatabasesInVector2", mysqlsee);
                }
            }

            rowCountCompare.add(new String(table2RowCount));

            if (rs1.getString(1).equals(table2RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }
            rowCountCompare.add(new String(tmp));
            tableRowCountCompare.add(rowCountCompare);
        }


        return tableRowCountCompare;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getRowCountComparisonsForThreeDatabasesInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2, Connection con3, DatabaseUtil dbUtil3) throws SQLException {
        Vector<Object> tableRowCountCompare = new Vector<Object>();
        String table2RowCount = "";
        String table3RowCount = "";
        Vector<String> tables = getTablesInVector(con1);

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();
        Statement stmt3 = con3.createStatement();

        Enumeration enumerate = tables.elements();
        String tmp = "";
        String dbUtil1Query = "";
        String dbUtil2Query = "";
        String dbUtil3Query = "";
        Vector<String> rowCountCompare = new Vector<String>(); //put here when used clear()
        while (enumerate.hasMoreElements()) {
            tmp = (String) enumerate.nextElement();
            dbUtil1Query = "";
            dbUtil2Query = "";
            dbUtil3Query = "";
            if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil1Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil2Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }
            if (dbUtil3.getVendor().equalsIgnoreCase("mysql")) {
                dbUtil3Query = "SELECT COUNT(*) FROM `" + tmp + "`";
            } else if (dbUtil3.getVendor().equalsIgnoreCase("mssql")) {
                dbUtil3Query = "SELECT COUNT(*) FROM [" + tmp + "]";
            }

            //Vector<String> rowCountCompare = new Vector<String>(); --replaced by clear()
            rowCountCompare.clear();
            rowCountCompare.add(new String(tmp));

            ResultSet rs1 = stmt1.executeQuery(dbUtil1Query);
            rs1.next();
            rowCountCompare.add(new String(rs1.getString(1)));


            try {
                ResultSet rs2 = stmt2.executeQuery(dbUtil2Query);
                rs2.next();
                table2RowCount = rs2.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table2RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table2RowCount = "Not Found";
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    Logs.write("getRowCountComparisonsForThreeDatabasesInVector", mysqlsee);
                }
            }

            rowCountCompare.add(new String(table2RowCount));
            if (rs1.getString(1).equals(table2RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }

            rowCountCompare.add(new String(tmp));

            try {
                ResultSet rs3 = stmt3.executeQuery(dbUtil3Query);
                rs3.next();
                table3RowCount = rs3.getString(1);
            } catch (SQLException mysqlsee) {
                if (mysqlsee.getMessage().indexOf("Table") != -1) {
                    table3RowCount = "Not Found";
                } else if (mysqlsee.getMessage().indexOf("object") != -1) {
                    table3RowCount = "Not Found";
                } else {
                    JOptionPane.showMessageDialog(null, mysqlsee.getMessage());
                    Logs.write("getRowCountComparisonsForThreeDatabasesInVector", mysqlsee);
                }
            }

            rowCountCompare.add(new String(table3RowCount));
            if (rs1.getString(1).equals(table3RowCount)) {
                tmp = "YES";
            } else {
                tmp = "NO";
            }

            rowCountCompare.add(new String(tmp));
            tableRowCountCompare.add(rowCountCompare);
        }
        return tableRowCountCompare;
    }

//--------------------------------------------------------------------------------
    public void closeConnection(Connection con) throws SQLException {
        con.close();
    }

    public String getVendor() {
        return vendor;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getColumnDataTypesInfoInVector(Connection con, DatabaseUtil db) throws SQLException {
        Vector<Object> columnInfo = new Vector<Object>();
        DatabaseMetaData mtd = con.getMetaData();
        ResultSet rs = mtd.getColumns(db.dbname, null, "%", "%");

        Vector<String> row = new Vector<String>(); //clear() method used inside loop
        while (rs.next()) {
            //Vector<String> row = new Vector<String>(); -- replaced with clear()
            row.clear();
            row.add(rs.getString("TABLE_NAME") + "." + rs.getString("COLUMN_NAME"));
            row.add(rs.getString("TYPE_NAME") + "(" + rs.getString("COLUMN_SIZE") + ")");
            columnInfo.add(row);
        }

        return columnInfo;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getColumnDataTypesComparisonForTwoDatabaseInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2) throws SQLException {
        Vector<Object> columnInfo = new Vector<Object>();
        Vector<String> table = dbUtil1.getTablesInVector(con1);
        DatabaseMetaData mtd1 = con1.getMetaData();
        DatabaseMetaData mtd2 = con2.getMetaData();
        Enumeration enumerate = table.elements();

        //Vector<String> row = new Vector<String>(); //made use of clear() in loop
        String temp = "";

        while (enumerate.hasMoreElements()) {
            temp = (String) enumerate.nextElement();
            ResultSet rs1 = mtd1.getColumns(dbUtil1.dbname, null, temp, "%");

            while (rs1.next()) {
                Vector<String> row = new Vector<String>(); //--replace with clear()
                //row.clear();
                row.add(rs1.getString("TABLE_NAME") + "." + rs1.getString("COLUMN_NAME"));
                row.add(rs1.getString("TYPE_NAME") + "(" + rs1.getString("COLUMN_SIZE") + ")");
                //System.out.println(row);

                try {
                    ResultSet rs2 = mtd2.getColumns(dbUtil2.dbname, null, temp, rs1.getString("COLUMN_NAME"));
                    rs2.next();
                    row.add(rs2.getString("TYPE_NAME") + "(" + rs2.getString("COLUMN_SIZE") + ")");

                    if ((rs1.getString("TYPE_NAME") + rs1.getString("COLUMN_SIZE")).equalsIgnoreCase((rs2.getString("TYPE_NAME") + rs2.getString("COLUMN_SIZE")))) {
                        row.add(new String("OK"));
                    } else {
                        row.add(new String("DIFFERENT"));
                    }
                } catch (SQLException sqle) {
                    String errMysql = "Illegal operation on empty result set";
                    String errMssql = "The result set has no current row";

                    if (sqle.getMessage().indexOf(errMysql) != -1 || sqle.getMessage().indexOf(errMssql) != -1) {
                        row.add("MISSING");
                        row.add("NO");
                    } else {
                        JOptionPane.showMessageDialog(null, sqle.getMessage());
                        Logs.write("getColumnDataTypesComparisonForTwoDatabaseInVector", sqle);
                    }
                }

                columnInfo.add(row);
            }
        }
        return columnInfo;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getColumnDataTypesComparisonForTwoDatabaseInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2, JProgressBar bar1, JProgressBar bar2) throws SQLException {
        Vector<Object> columnInfo = new Vector<Object>();
        Vector<String> table = dbUtil1.getTablesInVector(con1);
        DatabaseMetaData mtd1 = con1.getMetaData();
        DatabaseMetaData mtd2 = con2.getMetaData();

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        setMySQLSession(stmt1);
        setMySQLSession(stmt2);

        Enumeration enumerate = table.elements();

        //Vector<String> row = new Vector<String>(); //made use of clear() in loop
        String temp = "";
        int i = 0;
        int limit = table.size();

        while (enumerate.hasMoreElements()) {
            temp = (String) enumerate.nextElement();
            ResultSet rs1 = mtd1.getColumns(dbUtil1.dbname, null, temp, "%");

            while (rs1.next()) {
                Vector<String> row = new Vector<String>(); //--replace with clear()
                //row.clear();
                row.add(rs1.getString("TABLE_NAME") + "." + rs1.getString("COLUMN_NAME"));
                row.add(rs1.getString("TYPE_NAME") + "(" + rs1.getString("COLUMN_SIZE") + ")");
                //System.out.println(row);

                try {
                    ResultSet rs2 = mtd2.getColumns(dbUtil2.dbname, null, temp, rs1.getString("COLUMN_NAME"));
                    rs2.next();
                    row.add(rs2.getString("TYPE_NAME") + "(" + rs2.getString("COLUMN_SIZE") + ")");

                    if ((rs1.getString("TYPE_NAME") + rs1.getString("COLUMN_SIZE")).equalsIgnoreCase((rs2.getString("TYPE_NAME") + rs2.getString("COLUMN_SIZE")))) {
                        row.add(new String("OK"));
                    } else {
                        row.add(new String("DIFFERENT"));
                    }
                } catch (SQLException sqle) {
                    String errMysql = "Illegal operation on empty result set";
                    String errMssql = "The result set has no current row";

                    if (sqle.getMessage().indexOf(errMysql) != -1 || sqle.getMessage().indexOf(errMssql) != -1) {
                        row.add("MISSING");
                        row.add("NO");
                    } else {
                        JOptionPane.showMessageDialog(null, sqle.getMessage());
                        Logs.write("getColumnDataTypesComparisonForTwoDatabaseInVector", sqle);
                    }
                }

                columnInfo.add(row);
            }
            i++;
            DBInfo.updateProgressStatus(bar1, i, limit);
            DBInfo.updateProgressStatus(bar2, i, limit);
            //pause();
        }
        DBInfo.resetProgressBars();
        return columnInfo;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getColumnDataTypesComparisonForTwoDatabaseInVector(Connection con1, DatabaseUtil dbUtil1, Connection con2, DatabaseUtil dbUtil2, boolean ignoreTablesSelected, JProgressBar bar1, JProgressBar bar2) throws SQLException {
        Vector<Object> columnInfo = new Vector<Object>();
        Vector<String> tables = dbUtil1.getTablesInVector(con1);
        Vector<String> specifiedTables = VectorUtil.getListOfIgnoreTables("specified_tables.csv");
        if(specifiedTables.size() != 0)
        {            
            if(ignoreTablesSelected == true)
            {
                Logs.write("Ignoring specified tables from Primary DB for compare");
                tables.removeAll(specifiedTables);
            }
            else
            {
                Logs.write("Considering specified tables only from Primary DB for compare");
                tables = specifiedTables;
            }
        }
        else
        {
            Logs.write("Considering all tables from Primary DB for compare");
        }

        DatabaseMetaData mtd1 = con1.getMetaData();
        DatabaseMetaData mtd2 = con2.getMetaData();

        Statement stmt1 = con1.createStatement();
        Statement stmt2 = con2.createStatement();

        setMySQLSession(stmt1);
        setMySQLSession(stmt2);

        Enumeration enumerate = tables.elements();

        //Vector<String> row = new Vector<String>(); //made use of clear() in loop
        String temp = "";
        int i = 0;
        int limit = tables.size();

        while (enumerate.hasMoreElements()) {
            temp = (String) enumerate.nextElement();
            ResultSet rs1 = mtd1.getColumns(dbUtil1.dbname, null, temp, "%");

            while (rs1.next()) {
                Vector<String> row = new Vector<String>(); //--replace with clear()
                //row.clear();
                row.add(rs1.getString("TABLE_NAME") + "." + rs1.getString("COLUMN_NAME"));
                row.add(rs1.getString("TYPE_NAME") + "(" + rs1.getString("COLUMN_SIZE") + ")");
                //System.out.println(row);

                try {
                    ResultSet rs2 = mtd2.getColumns(dbUtil2.dbname, null, temp, rs1.getString("COLUMN_NAME"));
                    rs2.next();
                    row.add(rs2.getString("TYPE_NAME") + "(" + rs2.getString("COLUMN_SIZE") + ")");

                    if ((rs1.getString("TYPE_NAME") + rs1.getString("COLUMN_SIZE")).equalsIgnoreCase((rs2.getString("TYPE_NAME") + rs2.getString("COLUMN_SIZE")))) {
                        row.add(new String("OK"));
                    } else {
                        row.add(new String("DIFFERENT"));
                    }
                } catch (SQLException sqle) {
                    String errMysql = "Illegal operation on empty result set";
                    String errMssql = "The result set has no current row";

                    if (sqle.getMessage().indexOf(errMysql) != -1 || sqle.getMessage().indexOf(errMssql) != -1) {
                        row.add("MISSING");
                        row.add("NO");
                    } else {
                        JOptionPane.showMessageDialog(null, sqle.getMessage());
                        Logs.write("getColumnDataTypesComparisonForTwoDatabaseInVector", sqle);
                    }
                }

                columnInfo.add(row);
            }
            i++;
            DBInfo.updateProgressStatus(bar1, i, limit);
            DBInfo.updateProgressStatus(bar2, i, limit);
            //pause();
        }
        DBInfo.resetProgressBars();
        return columnInfo;
    }

//--------------------------------------------------------------------------------
    public Vector<String> getDatabaseNames(Connection con, String dbNamePattern) throws SQLException {
        Vector<String> dbNames = new Vector<String>();
        DatabaseMetaData mtd = con.getMetaData();
        ResultSet rs = mtd.getCatalogs();
        String tmp = "";
        while (rs.next()) {
            tmp = rs.getString(1);
            if (tmp.indexOf(dbNamePattern) != -1) {
                dbNames.add(rs.getString(1));
            }
        }
        return dbNames;
    }

//--------------------------------------------------------------------------------
    public Vector<String> getDatabaseSchemas(Connection con) throws SQLException {
        Vector<String> dbSchemas = new Vector<String>();
        DatabaseMetaData mtd = con.getMetaData();
        ResultSet rs = mtd.getSchemas();
        while (rs.next()) {
            dbSchemas.add(rs.getString(1));
        }
        return dbSchemas;
    }

//--------------------------------------------------------------------------------
    public Vector<String> getSupportedDataTypes(Connection con) throws SQLException {
        Vector<String> dataTypes = new Vector<String>();
        DatabaseMetaData mtd = con.getMetaData();
        ResultSet rs = mtd.getTypeInfo();
        while (rs.next()) {
            dataTypes.add(rs.getString(1));
        }
        return dataTypes;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getKeyPairFromTable(DatabaseUtil dbUtil, Connection con, String table, String keyCol, String valCol) throws SQLException {
        Vector<Object> keyPair = new Vector<Object>();
        Statement stmt = con.createStatement();
        String query = "";
        if (dbUtil.getVendor().equalsIgnoreCase("mysql")) {
            query = "SELECT " + keyCol + ", " + valCol + " FROM `" + table + "`";
        } else if (dbUtil.getVendor().equalsIgnoreCase("mssql")) {
            query = "SELECT " + keyCol + ", " + valCol + " FROM [" + table + "]";
        }

        ResultSet rs = stmt.executeQuery(query);

        Vector<String> row = new Vector<String>(); //made use of clear() in loop
        while (rs.next()) {
            //Vector<String> row = new Vector<String>(); --replace with clear()
            row.clear();
            row.add(new String(rs.getString(1)));
            row.add(new String(rs.getString(2)));
            keyPair.add(row);
        }

        return keyPair;
    }

//--------------------------------------------------------------------------------
    public Vector<Object> getKeyPairComparisonForTwoTable(DatabaseUtil dbUtil1, Connection con1, DatabaseUtil dbUtil2, Connection con2, String table, String keyCol, String valCol) throws SQLException {
        Vector<Object> keyPair = new Vector<Object>();
        Statement stmt1 = con1.createStatement();
        String query1 = "";
        if (dbUtil1.getVendor().equalsIgnoreCase("mysql")) {
            query1 = "SELECT " + keyCol + ", " + valCol + " FROM `" + table + "`";
        } else if (dbUtil1.getVendor().equalsIgnoreCase("mssql")) {
            query1 = "SELECT " + keyCol + ", " + valCol + " FROM [" + table + "]";
        }

        ResultSet rs1 = stmt1.executeQuery(query1);

        Vector<String> row = new Vector<String>(); // made use of clear() in loop
        while (rs1.next()) {
            //Vector<String> row = new Vector<String>();
            row.clear();
            row.add(new String(rs1.getString(1)));
            row.add(new String(rs1.getString(2)));

            Statement stmt2 = con2.createStatement();
            String query2 = "";
            if (dbUtil2.getVendor().equalsIgnoreCase("mysql")) {
                query2 = "SELECT " + valCol + " FROM `" + table + "` WHERE " + keyCol + " = \'" + rs1.getString(1) + "\'";
            } else if (dbUtil2.getVendor().equalsIgnoreCase("mssql")) {
                query2 = "SELECT " + valCol + " FROM [" + table + "] WHERE " + keyCol + " = \'" + rs1.getString(1) + "\'";
            }
            try {
                ResultSet rs2 = stmt2.executeQuery(query2);
                rs2.next();
                row.add(new String(rs2.getString(1)));
                if (rs1.getString(2).equalsIgnoreCase(rs2.getString(1))) {
                    row.add(new String("OK"));
                } else {
                    row.add(new String("Different"));
                }
            } catch (SQLException sqle) {
                String errMysql = "Illegal operation on empty result set";
                String errMssql = "The result set has no current row";

                if (sqle.getMessage().indexOf(errMysql) != -1 || sqle.getMessage().indexOf(errMssql) != -1) {
                    row.add("Missing");
                } else {
                    JOptionPane.showMessageDialog(null, sqle.getMessage());
                    Logs.write("getKeyPairComparisonForTwoTable", sqle);
                }
                row.add("Different");
            }

            keyPair.add(row);
        }

        return keyPair;
    }

//--------------------------------------------------------------------------------
    public static void pause() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
    }

//--------------------------------------------------------------------------------
    public static void analysingConnection(Connection con) throws SQLException {
        start = System.currentTimeMillis();
        while (!con.isClosed()) {
            System.out.println("Alive");
            pause();
        }
        stop = System.currentTimeMillis();
        System.out.println("Closed: was alive for " + (stop - start) + " ms");
    }
//--------------------------------------------------------------------------------
//--------------------------------------------------------------------------------
    /**public static void main(String args[])
    {
    try
    {
    DatabaseUtil dbUtil1 = new DatabaseUtil();
    dbUtil1.setDbInfo(new File("primarydb.inf"));
    DatabaseUtil dbUtil2 = new DatabaseUtil();
    dbUtil2.setDbInfo(new File("secondarydb2.inf"));
    DatabaseUtil dbUtil3 = new DatabaseUtil();
    dbUtil3.setDbInfo(new File("secondarydb1.inf"));
    final Connection con1 = dbUtil1.getConnection();
    Connection con2 = dbUtil2.getConnection();
    Connection con3 = dbUtil3.getConnection();

    System.out.println("DBUtil1 db info: " + dbUtil1.showDbInfo());
    System.out.println("DBUtil2 db info: " + dbUtil2.showDbInfo());
    System.out.println("DBUtil3 db info: " + dbUtil3.showDbInfo());
    System.out.println("------------------------");
    //System.out.println(dbUtil1.getDatabaseNames(con1, "nor"));
    //System.out.println(dbUtil2.getDatabaseNames(con2, "nor"));
    //System.out.println(dbUtil3.getDatabaseNames(con3, "nor"));
    //System.out.println(dbUtil1.getSupportedDataTypes(con1));
    //System.out.println(dbUtil2.getSupportedDataTypes(con2));
    //System.out.println(dbUtil3.getSupportedDataTypes(con3));
    //dbUtil1.testConnection();
    //dbUtil2.testConnection();
    //dbUtil3.testConnection();
    new Thread(new Runnable()
    {
    public void run()
    {
    try{
    DatabaseUtil.analysingConnection(con1);
    }
    catch(SQLException sqle)
    {
    }

    }
    });
    Vector<String> header = new Vector<String>();
    header.add(new String("Tables"));
    header.add(new String("RowCounts"));
    header.add(new String("Tables"));
    header.add(new String("RowCounts"));
    //Vector<Object> data = new Vector<Object>();
    /*Vector<String> tables = dbUtil1.getTablesInVector(con1);
    data.add(tables);
    VectorUtil.printVectorResultInConsole(data);
    VectorUtil.storeVectorResultInCSVFile(header, data, new File("test_output.csv"));*/

    /*Vector<Object> tableRowCounts = dbUtil1.getRowCountForTablesInVector(con1);
    VectorUtil.printVectorResultInConsole(tableRowCounts);
    VectorUtil.storeVectorResultInCSVFile(header, tableRowCounts, new File("test_output.csv"));
    start = System.currentTimeMillis();
    //Vector<Object> tableRowCountCompare2 = new Vector<Object>(100);
    for(int i=0; i < 100; i++)
    {
    Vector<Object>
    tableRowCountCompare2 = dbUtil1.getRowCountComparisonsForTwoDatabasesInVector(con1, dbUtil1, con2, dbUtil2);
    }
    stop = System.currentTimeMillis();
    System.out.println("Took time in ms : " + (stop - start));
    //VectorUtil.storeVectorResultInCSVFile(header, tableRowCountCompare2, new File("test_output.csv"));
    //Vector<Object> tableRowCountCompare3 = dbUtil1.getRowCountComparisonsForThreeDatabasesInVector(con1, dbUtil1, con2, dbUtil2, con3, dbUtil3);
    /*VectorUtil.printVectorResultInConsole(tableRowCountCompare3);
    VectorUtil.storeVectorResultInCSVFile(header, tableRowCountCompare3, new File("test_output.csv"));*/

    /*Vector<Object> filter = VectorUtil.filteredResultVector(tableRowCountCompare3, 5, "NO");
    VectorUtil.printVectorResultInConsole(filter);
    VectorUtil.storeVectorResultInCSVFile(header, filter, new File("test_output.csv"));*/
    //String temp[] = dbUtil1.getTablesInArray(con1);
    //for(String tmp : temp)
    //{
    //	System.out.println(tmp);
    //}
    //Vector<String> header = new Vector<String>();
    //header.add(new String("TABLE_NAME"));
    //header.add(new String("ROW_COUNTS"));
    //header.add(new String("DATA_TYPE_DB2"));
    //header.add(new String("RESULT"));
    //Vector<Object> columnComparison = dbUtil1.getColumnDataTypesComparisonForTwoDatabaseInVector(con1, dbUtil1, con2, dbUtil2);
    //VectorUtil.printVectorResultInConsole(columnComparison);
    //VectorUtil.storeVectorResultInCSVFile(header, columnComparison, new File("test_output.csv"));
    //start = System.currentTimeMillis();
    //String[][] rowCounts1;
    //for(int i = 0; i < 1000; i++)
    //	rowCounts1 = DatabaseUtil.getRowCountForTablesInArray(con1, dbUtil1);
    //stop = System.currentTimeMillis();
    //VectorUtil.storeVectorResultInCSVFile(header, rowCounts1, new File("test_output.csv"));
    //System.out.println("Took time in ms : " + (stop - start));
    //start = System.currentTimeMillis();
    //Vector<Object> rowCounts2;
    //for(int i = 0; i < 1000; i++)
    //	rowCounts2 = dbUtil1.getRowCountForTablesInVector(con1);
    //stop = System.currentTimeMillis();
    //System.out.println("Took time in ms : " + (stop - start));*/

    /*dbUtil1.closeConnection(con1);
    dbUtil2.closeConnection(con2);
    dbUtil3.closeConnection(con3);

    }
    catch(ClassNotFoundException cnfe)
    {
    JOptionPane.showMessageDialog(null,cnfe.getMessage());
    Logs.write("main method", cnfe);
    System.exit(0);
    }
    catch(SQLException sqle)
    {
    JOptionPane.showMessageDialog(null,sqle.getMessage());
    Logs.write("main method", sqle);
    System.exit(0);
    }
    }*/
}
