import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import javax.swing.JOptionPane;

public class MySQLData {

    Connection con = null;
    DatabaseUtil dbutil = null;

    public MySQLData() {
    }

    public Connection useConnection() {
        if (con == null) {
            JOptionPane.showMessageDialog(null, "MySQLData Object - Connection has not been set. Process cannot be continued...");
            System.exit(0);
        }

        return con;
    }

    public DatabaseUtil useDatabaseUtil() {
        if (dbutil == null) {
            JOptionPane.showMessageDialog(null, "MySQLData Object - DB Info has not been set. Process cannot be continued...");
            System.exit(0);
        }

        return dbutil;
    }

    public void setConnection(String dbInfoFileName) {
        dbutil = new DatabaseUtil(dbInfoFileName);
        try {
            con = dbutil.getConnection();
        } catch (ClassNotFoundException cnfe) {
            Logs.write("MySQLData Object - Unable to locate connection libraries", cnfe);
        } catch (SQLException sqle) {
            Logs.write("MySQLData Object - Unable to set connection", sqle);
        }
    }

    public void closeConnection() {
        try {
            dbutil.closeConnection(con);
        } catch (SQLException sqle) {
            Logs.write("MySQLData Object - Unable to close connection", sqle);
        }
    }

    public LinkedHashMap<String, String> getColumnNameAndTypeListForTable(String tableName) {
        LinkedHashMap<String, String> dataTypes = new LinkedHashMap<String, String>();
        try {
            DatabaseMetaData dbmd = con.getMetaData();
            ResultSet rs = dbmd.getColumns(dbutil.dbname, null, tableName, "%");
            while (rs.next()) {
                dataTypes.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
            }
        } catch (SQLException sqle) {
            Logs.write("MySQL Data - Unable to store column and its type in memory", sqle);
        }
        return dataTypes;
    }

    public LinkedHashMap<String, String> getColumnIdAndTypeListForTable(String tableName) {
        LinkedHashMap<String, String> dataTypes = new LinkedHashMap<String, String>();
        try {
            DatabaseMetaData dbmd = con.getMetaData();
            ResultSet rs = dbmd.getColumns(dbutil.dbname, null, tableName, "%");
            int colCount = 1;
            while (rs.next()) {
                dataTypes.put(new String("" + colCount), rs.getString("TYPE_NAME"));
                colCount = colCount + 1;
            }
        } catch (SQLException sqle) {
            Logs.write("MySQL Data - Unable to store column and its type in memory", sqle);
        }
        return dataTypes;
    }

    public StringBuffer dataUpdateCondition(LinkedHashMap<String, String> dataTypes) {
        StringBuffer selectQuery = new StringBuffer();

        return selectQuery;
    }

    public StringBuffer getFirstNRowsData(String tableName, int nRow, boolean writeDataContents) {
        StringBuffer dataBuffer = new StringBuffer();

        try {
            Statement stmt = con.createStatement();
            //stmt.setFetchSize(100);
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tableName + "`");
            dataBuffer.append(getDataInStringBufferFirstNRows(tableName, rs, nRow));
            rs = null;
            stmt = null;
        } catch (SQLException sqle) {
            String message = sqle.getMessage();
            if (message.length() >= 13 & message.indexOf("doesn't exist") != -1) {
                dataBuffer.append("Table Not Found");
                Logs.write("MsSQL Object - Table " + tableName + " Not Found, MISSING", sqle);
            } else {
                Logs.write("MySQL Object - Unable to get firt row from table " + tableName, sqle);
            }
        }
        if(writeDataContents)
        {
        writeStringBufferToFile("files/" + tableName + ".txt", dataBuffer);
        }
        
        return dataBuffer;
    }

    public StringBuffer getReverseNRowsData(String tableName, int nRow, boolean writeDataContents) {
        StringBuffer dataBuffer = new StringBuffer();

        try {
            Statement stmt = con.createStatement();
            stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
            //stmt.setFetchSize(100);
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tableName + "` limit " + nRow);
            dataBuffer.append(getDataInStringBufferFirstNRows(tableName, rs, nRow));
            rs = null;
            stmt = null;
        } catch (SQLException sqle) {
            String message = sqle.getMessage();
            if (message.length() >= 13 & message.indexOf("doesn't exist") != -1) {
                dataBuffer.append("Table Not Found");
                Logs.write("MsSQL Object - Table " + tableName + " Not Found, MISSING", sqle);
            } else {
                Logs.write("MySQL Object - Unable to get firt row from table " + tableName, sqle);
            }
        }
        if(writeDataContents)
        {
        writeStringBufferToFile("files/" + tableName + ".txt", dataBuffer);
        }
        return dataBuffer;
    }

    public StringBuffer getLastNRowsData(String tableName, int lastNRows, boolean writeDataContents) {
        StringBuffer dataBuffer = new StringBuffer();

        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tableName + "`");
            dataBuffer.append(getDataInStringBufferLastNRows(tableName, rs, lastNRows));
            rs = null;
            stmt = null;
        } catch (SQLException sqle) {
            String message = sqle.getMessage();
            if (message.length() >= 13 & message.indexOf("doesn't exist") != -1) {
                dataBuffer.append("Table Not Found");
                Logs.write("MsSQL Object - Table " + tableName + " Not Found, MISSING", sqle);
            } else {
                Logs.write("MySQL Object - Unable to get last row from table " + tableName, sqle);
            }
        }
        if(writeDataContents)
        {
        writeStringBufferToFile("files/" + tableName + ".txt", dataBuffer);
        }
        return dataBuffer;
    }

    public StringBuffer getRandomNthRowData(String tableName, int nThRowPer100, boolean writeDataContents) {
        StringBuffer dataBuffer = new StringBuffer();
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM `" + tableName + "`");
            dataBuffer.append(getDataInStringBufferRandomNthRows(tableName, rs, nThRowPer100));
            rs = null;
            stmt = null;
        } catch (SQLException sqle) {
            String message = sqle.getMessage();
            if (message.length() >= 13 & message.indexOf("doesn't exist") != -1) {
                dataBuffer.append("Table Not Found");
                Logs.write("MsSQL Object - Table " + tableName + " Not Found, MISSING", sqle);
            } else {
                Logs.write("MySQL Object - Unable to get rows from table " + tableName, sqle);
            }
        }
        if(writeDataContents)
        {
            writeStringBufferToFile("files/" + tableName + ".txt", dataBuffer);
        }
        return dataBuffer;
    }

    public StringBuffer getDataInStringBufferGeneral(ResultSet rs) {
        StringBuffer dataBuffer = new StringBuffer();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    dataBuffer.append(rs.getString(i));
                }
            }
        } catch (SQLException sqle) {
            Logs.write("MySQLData - Unable to convert resultset data to required format", sqle);
        }
        return dataBuffer;
    }

    public StringBuffer getDataInStringBuffer(String tableName, ResultSet rs) {
        StringBuffer dataBuffer = new StringBuffer();
        LinkedHashMap<String, String> dataTypes = getColumnIdAndTypeListForTable(tableName);

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    try {
                        if (dataTypes.get("" + i).equalsIgnoreCase("date")) {
                            try {
                                dataBuffer.append(updatedDataForDateType(rs.getString(i)));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 23 & ((message.indexOf("0000-00-00") != -1) && (message.indexOf("java.sql.Date") != -1))) {
                                    dataBuffer.append(updatedDataForDateType("0000-00-00"));
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("time")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("datetime")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("timestamp")) {
                            try {
                                dataBuffer.append(rs.getString(i));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 28 & ((message.indexOf("0000-00-00 00:00:00") != -1) && (message.indexOf("TIMESTAMP") != -1))) {
                                    dataBuffer.append("0000-00-00 00:00:00");
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else {
                            dataBuffer.append(rs.getString(i));
                        }
                    } catch (NullPointerException npe) {
                        Logs.write("MySQLData Object - Null Pointer Exception at col " + i, npe);
                    }

                    dataBuffer.append(" $ ");
                }
                dataBuffer.append(" || ");
            }
        } catch (SQLException sqle) {
            Logs.write("MySQLData - Unable to convert resultset data to required format", sqle);
        }
        return dataBuffer;
    }

    public StringBuffer getDataInStringBufferFirstNRows(String tableName, ResultSet rs, int firstNRows) {
        StringBuffer dataBuffer = new StringBuffer();
        LinkedHashMap<String, String> dataTypes = getColumnIdAndTypeListForTable(tableName);
        int cnt = 0;
        try {
            rs.setFetchSize(100);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();
            while (rs.next()) {
                if (cnt >= firstNRows) {
                    break;
                }

                for (int i = 1; i <= colCount; i++) {
                    try {
                        if (dataTypes.get("" + i).equalsIgnoreCase("date")) {
                            try {
                                dataBuffer.append(updatedDataForDateType(rs.getString(i)));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 23 & ((message.indexOf("0000-00-00") != -1) && (message.indexOf("java.sql.Date") != -1))) {
                                    dataBuffer.append(updatedDataForDateType("0000-00-00"));
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("time")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("datetime")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("timestamp")) {
                            try {
                                dataBuffer.append(updateDataForTimestampType(rs.getString(i)));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 28 & ((message.indexOf("0000-00-00 00:00:00") != -1) && (message.indexOf("TIMESTAMP") != -1))) {
                                    dataBuffer.append("0000-00-00 00:00:00");
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else {
                            dataBuffer.append(rs.getString(i));
                        }
                    } catch (NullPointerException npe) {
                        Logs.write("MySQLData Object - Null Pointer Exception at col " + i, npe);
                    }
                }
                cnt++;
            }
        } catch (SQLException sqle) {
            Logs.write("MySQLData - Unable to convert resultset data to required format", sqle);
        }
        return dataBuffer;
    }

    public StringBuffer getDataInStringBufferLastNRows(String tableName, ResultSet rs, int lastNRows) {
        StringBuffer dataBuffer = new StringBuffer();
        LinkedHashMap<String, String> dataTypes = getColumnIdAndTypeListForTable(tableName);

        try {
            int rowCount = getRowCount(tableName);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();

            rs.afterLast();

            for (int nCount = 1; nCount <= lastNRows; nCount++) {
                if (rs.previous()) {

                    for (int i = 1; i <= colCount; i++) {
                        try {
                            if (dataTypes.get("" + i).equalsIgnoreCase("date")) {
                                try {
                                    dataBuffer.append(updatedDataForDateType(rs.getString(i)));
                                } catch (Exception e) {
                                    String message = e.getMessage();
                                    if (message.length() >= 23 & ((message.indexOf("0000-00-00") != -1) && (message.indexOf("java.sql.Date") != -1))) {
                                        dataBuffer.append(updatedDataForDateType("0000-00-00"));
                                    } else {
                                        Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                    }
                                }
                            } else if (dataTypes.get("" + i).equalsIgnoreCase("time")) {
                                dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                            } else if (dataTypes.get("" + i).equalsIgnoreCase("datetime")) {
                                dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                            } else if (dataTypes.get("" + i).equalsIgnoreCase("timestamp")) {
                                try {
                                    dataBuffer.append(updateDataForTimestampType(rs.getString(i)));
                                } catch (Exception e) {
                                    String message = e.getMessage();
                                    if (message.length() >= 28 & ((message.indexOf("0000-00-00 00:00:00") != -1) && (message.indexOf("TIMESTAMP") != -1))) {
                                        dataBuffer.append("0000-00-00 00:00:00");
                                    } else {
                                        Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                    }
                                }
                            } else {
                                dataBuffer.append(rs.getString(i));
                            }
                        } catch (NullPointerException npe) {
                            Logs.write("MySQLData Object - Null Pointer Exception at col " + i, npe);
                        }
                    }

                }
            }

        } catch (SQLException sqle) {
            Logs.write("MySQLData - Unable to convert resultset data to required format", sqle);
        }
        return dataBuffer;
    }

    public StringBuffer getDataInStringBufferRandomNthRows(String tableName, ResultSet rs, int nThRowPer100) {
        StringBuffer dataBuffer = new StringBuffer();
        LinkedHashMap<String, String> dataTypes = getColumnIdAndTypeListForTable(tableName);
        int nCount = 1;
        try {
            int rowCount = getRowCount(tableName);
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCount = rsmd.getColumnCount();

            if (rowCount <= 500) {
                nThRowPer100 = rowCount / 10;
            } else {
                nThRowPer100 = rowCount / 100 * nThRowPer100;
            }


            while (rs.next()) {
                if (nThRowPer100 != 0) {
                    while (nCount % nThRowPer100 != 0) {
                        nCount++;
                        rs.next();

                    }
                }
                nCount++;
                for (int i = 1; i <= colCount; i++) {
                    try {
                        if (dataTypes.get("" + i).equalsIgnoreCase("date")) {
                            try {
                                dataBuffer.append(updatedDataForDateType(rs.getString(i)));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 23 & ((message.indexOf("0000-00-00") != -1) && (message.indexOf("java.sql.Date") != -1))) {
                                    dataBuffer.append(updatedDataForDateType("0000-00-00"));
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("time")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("datetime")) {
                            dataBuffer.append(updateDataForTimeType(rs.getString(i)));
                        } else if (dataTypes.get("" + i).equalsIgnoreCase("timestamp")) {
                            try {
                                dataBuffer.append(updateDataForTimestampType(rs.getString(i)));
                            } catch (Exception e) {
                                String message = e.getMessage();
                                if (message.length() >= 28 & ((message.indexOf("0000-00-00 00:00:00") != -1) && (message.indexOf("TIMESTAMP") != -1))) {
                                    dataBuffer.append("0000-00-00 00:00:00");
                                } else {
                                    Logs.write("MySQLData Object - error while retrieving data for at col " + i + " table " + tableName, e);
                                }
                            }
                        } else {
                            dataBuffer.append(rs.getString(i));
                        }
                    } catch (NullPointerException npe) {
                        Logs.write("MySQLData Object - Null Pointer Exception at col " + i, npe);
                    }
                }
            }
        } catch (SQLException sqle) {
            Logs.write("MySQLData - Unable to convert resultset data to required format", sqle);
        }
        return dataBuffer;
    }

    public int getRowCount(String tableName) {
        int rowCount;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM `" + tableName + "`");
            rs.next();
            rowCount = Integer.parseInt(rs.getString(1));
        } catch (SQLException sqle) {
            Logs.write("MySQLData Object - Unable to get row count for table " + tableName, sqle);
            return 0;
        }
        return rowCount;
    }

    public void writeStringBufferToFile(String fileName, StringBuffer sbuff) {
        try {
            //PrintWriter file = new PrintWriter(new File(fileName));
            //file.write(sbuff.toString());
            //file.flush();
            //file.close();
            
            RandomAccessFile raf = new RandomAccessFile(fileName,"rw");
            raf.seek(raf.length());
            raf.writeBytes(sbuff.toString());
            raf.writeBytes("\n=======================================\n");
            raf.close();
            
        } catch (FileNotFoundException fnfe) {
            Logs.write("MsSQLData - Unable to write data to file: " + fileName, fnfe);
        } catch (IOException ioe) {
        	Logs.write("MsSQLData - Unable to write data to file: " + fileName, ioe);
        }
        
    }

    public String updatedDataForDateType(String data) {
        if (data.equalsIgnoreCase("null")) {
            return "null";
        }
        return data + " 00:00:00";
    }

    public String updateDataForTimeType(String data) {
        if (data.equalsIgnoreCase("null")) {
            return "null";
        }
        return "1900-01-01 " + data;
    }

    public String updateDataForTimestampType(String data) {
        if (data.length() >= 1 & (data.indexOf(".") != -1)) {
            return data.substring(0, data.indexOf("."));
        }

        return data;
    }

    @Override
    public void finalize() {
        closeConnection();
        Logs.write("Cleaning mysql object");
    }

    /*  public static void main(String args[])
    {
    if (args.length < 1)
    {
    Logs.write("USAGE: java MySQLData <table-name>");
    System.exit(0);
    }
    MySQLData mysqlO = new MySQLData();
    mysqlO.setConnection("primarydb.inf");
    //StringBuffer sbuff = mysqlO.getFirstNRowsData(args[0], 2);
    //StringBuffer sbuff = mysqlO.getLastNRowsData(args[0], 2);
    StringBuffer sbuff = mysqlO.getRandomNthRowData(args[0], 10);

    //Iterator itr = dataTypes.entrySet().iterator();
    //while(itr.hasNext())
    //{
    //	Map.Entry entry = (Map.Entry) itr.next();
    //	System.out.print(entry.getKey() + ", ");
    //	Logs.write(entry.getValue());
    //}
    //StringBuffer sbuff = mysqlO.getFirstRowData(args[0]);
    Logs.write(sbuff.toString());
    mysqlO.writeStringBufferToFile(args[0] + ".txt", sbuff);
    mysqlO.closeConnection();
    System.gc();


    }
     * */
}
