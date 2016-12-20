
import java.util.*;
import java.sql.*;
import java.io.*;
import javax.swing.SwingUtilities;
import java.util.LinkedList;
import java.io.File;

public class DataCheck {

    boolean writeDataContents = false;
    //PrintWriter out;
    static int N = 0;
    Vector<String> headVector;
    String optionName;

    public DataCheck(String optionName) {

        writeDataContents = DBInfo.isWriteComparedContentsRequested();
        if (writeDataContents) {
            File tmpDataWrite = new File("files");
            if (!tmpDataWrite.exists()) {
                tmpDataWrite.mkdir();
            }
        }
        //try {
        this.optionName = optionName;
        //out = new PrintWriter(optionName);
        headVector = new Vector<String>();
        headVector.add("Table Name");
        //headVector.add("MD5 for DB1");
        //headVector.add("MD5 for DB2");
        headVector.add("Result");
        //} catch (FileNotFoundException fnfe) {
        //    Logs.write("DataCheck Object - unable to create and edit results file " + optionName, fnfe);
        //}
    }

    public static void usageHelp() {
        System.out.println("Usage: java DataCheck arg1 arg2");
        System.out.println();
        System.out.println("arg1: ");
        System.out.println("	 - firstNRows");
        System.out.println("	 - lastNRows");
        System.out.println("	 - randomNthRow");
        System.out.println();
        System.out.println("arg2: ");
        System.out.println("	 - (N): represents an integer");
        System.exit(0);
    }

    public LinkedList<String> getTableNamesFromFile(String tableListFile) {
        Scanner read;
        LinkedList<String> list = new LinkedList<String>();
        try {
            read = new Scanner(new File(tableListFile));
            list = new LinkedList<String>();
            while (read.hasNext()) {
                list.add(read.next());
            }
        } catch (IOException ioe) {
            Logs.write("DataCheck Object: skiptables.txt not found, no tables will be skipped");
        }

        if (list.size() == 0) {
            Logs.write("DataCheck Object: skiptables.txt is empty, all tables will be processed");
        }
        Logs.write("DataCheck Object: retrieved table names from file " + tableListFile);

        return list;
    }

    public void start(final String processType, int rcount) {
        final Timer t = new Timer();
        t.start();
        final Vector<Object> dataVector = new Vector<Object>();

        try {
            N = rcount;
        } catch (NumberFormatException nfe) {
            usageHelp();
            Logs.write("DataCheck Object - Number format exception for DataCheck row count", nfe);
        }

        MySQLData objMy1 = null;
        MsSQLData objMs1 = null;
        MySQLData objMy2 = null;
        MsSQLData objMs2 = null;
        String vendor1 = null;
        String vendor2 = null;
        String dbname1 = null;
        String dbname2 = null;

        Vector<String> tableNames = null;
        DataCheck dc = new DataCheck("results.csv");

        DatabaseUtil dbutil;
        Connection con;


        try {
            dbutil = new DatabaseUtil();
            dbutil.setDbInfo(new File("primarydb.inf"));
            con = dbutil.getConnection();
            tableNames = dbutil.getTablesInVector(con);
            dbutil.closeConnection(con);

            vendor1 = dbutil.vendor;
            dbname1 = dbutil.dbname;
            //System.out.println("Vendor for DB OBject 1 is " + vendor1);
            Logs.write("Vendor for DB OBject 1 is " + vendor1);
            dbutil.setDbInfo(new File("secondarydb.inf"));
            vendor2 = dbutil.vendor;
            dbname2 = dbutil.dbname;
            //System.out.println("Vendor for DB OBject 2 is " + vendor2);
            Logs.write("Vendor for DB OBject 2 is " + vendor2);

        } catch (ClassNotFoundException cnfe) {
            Logs.write("DataCheck Object - DB connection library not found", cnfe);
        } catch (SQLException sqle) {
            Logs.write("DataCheck Object - Error while retrieving table names", sqle);
        }

        int i = 0;
        int limit = tableNames.size();

        //dc.out.write("TABLE_NAME, " + vendor1 + "_" + dbname1 + ", " + vendor2 + "_" + dbname2 + ", results");
        //dc.out.write("\n");
        LinkedList<String> skipTables = getTableNamesFromFile("skiptables.txt");
        LinkedList<String> selectedTables = getTableNamesFromFile("selectedtables.txt");

        Enumeration enumerate = tableNames.elements();

        while (enumerate.hasMoreElements()) {
            Vector<String> dataRow = new Vector<String>();
            String val1 = null;
            String val2 = null;
            String table = (String) enumerate.nextElement();
            //dc.out.write(table);

            if (skipTables.contains(table)) {
                Logs.write("Skipping table: " + table + " listed in skiptables list");
                continue;
            }

            if (selectedTables.size() != 0 & (!selectedTables.contains(table))) {
                Logs.write("Ignoring table: " + table + " not listed in selectedtables list");
                continue;
            }

            dataRow.add(table);
            // System.out.println("Processing table: " + table);
            Logs.write("Processing table: " + table);
            //dc.out.write(", ");

            if (vendor1.equalsIgnoreCase("mysql")) {
                objMy1 = new MySQLData();
                objMy1.setConnection("primarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getLastNRowsData(table, N, writeDataContents));
                    //val1 = MD5Util.getMD5FromStringBuffer(objMy1.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val1);
                //dataRow.add(val1);
                objMy1.closeConnection();
                objMy1 = null;
            } else if (vendor1.equalsIgnoreCase("mssql")) {
                objMs1 = new MsSQLData();
                objMs1.setConnection("primarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getLastNRowsData(table, N, writeDataContents));
                    //val1 = MD5Util.getMD5FromStringBuffer(objMs1.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val1);
                //dataRow.add(val1);
                objMs1.closeConnection();
                objMs1 = null;
            }

            // dc.out.write(", ");

            if (vendor2.equalsIgnoreCase("mysql")) {
                objMy2 = new MySQLData();
                objMy2.setConnection("secondarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getLastNRowsData(table, N, writeDataContents));
                    //val2 = MD5Util.getMD5FromStringBuffer(objMy2.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val2);
                //dataRow.add(val2);
                objMy2.closeConnection();
                objMy2 = null;
            } else if (vendor2.equalsIgnoreCase("mssql")) {
                objMs2 = new MsSQLData();
                objMs2.setConnection("secondarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getLastNRowsData(table, N, writeDataContents));
                    //val2 = MD5Util.getMD5FromStringBuffer(objMs2.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getRandomNthRowData(table, N, writeDataContents));
                }

                //dc.out.write(val2);
                //dataRow.add(val2);
                objMs2.closeConnection();
                objMs2 = null;
            }


            //dc.out.write(", ");
            if (val1.equals(val2)) {
                if (val1.equals("d41d8cd98f00b204e9800998ecf8427e")) {
                    //dc.out.write("Both Empty");
                    dataRow.add("Both Empty");
                } else {
                    //dc.out.write("Matching");
                    dataRow.add("Matching");
                }
            } else {
                if (val2.equals("6420397f8502ace0bacf868c5936c11f")) {
                    //dc.out.write("Not Found");
                    dataRow.add("Not Found");
                } else {
                    //dc.out.write("Data Difference");
                    dataRow.add("Data Difference");
                }
            }

            //dc.out.write("\n");
            //dc.out.flush();
            dataVector.add(dataRow);
            i++;
            DBInfo.updateProgressStatus(DBInfo.getProgressBar1(), i, limit);
            DBInfo.updateProgressStatus(DBInfo.getProgressBar2(), i, limit);
        }
        t.stop();
        SwingUtilities.invokeLater(new Runnable() {

            public void run() {
                new TableFromFilteredVector("Compare " + processType, dataVector, headVector, t);
            }
        });
        VectorUtil.storeVectorResultInCSVFile(headVector, dataVector, new File("Compare_" + processType + "_" + DBInfo.getBriefName() + ".csv"));
        DBInfo.resetProgressBars();
        //dc.out.close();

    }

    public void start(final String processType, boolean ignoreTablesSelected, int rcount) {
        final Timer t = new Timer();
        t.start();
        final Vector<Object> dataVector = new Vector<Object>();

        try {
            N = rcount;
        } catch (NumberFormatException nfe) {
            usageHelp();
            Logs.write("DataCheck Object - Number format exception for DataCheck row count", nfe);
        }

        MySQLData objMy1 = null;
        MsSQLData objMs1 = null;
        MySQLData objMy2 = null;
        MsSQLData objMs2 = null;
        String vendor1 = null;
        String vendor2 = null;
        String dbname1 = null;
        String dbname2 = null;

        Vector<String> tableNames = null;
        //DataCheck dc = new DataCheck("results.csv");

        DatabaseUtil dbutil;
        Connection con;


        try {
            dbutil = new DatabaseUtil();
            dbutil.setDbInfo(new File("primarydb.inf"));
            con = dbutil.getConnection();
            tableNames = dbutil.getTablesInVector(con);
            dbutil.closeConnection(con);

            Vector<String> specifiedTables = VectorUtil.getListOfIgnoreTables("specified_tables.csv");
            if (specifiedTables.size() != 0) {
                if (ignoreTablesSelected == true) {
                    Logs.write("Ignoring specified tables only in Primary DB 1 for compare");
                    tableNames.removeAll(specifiedTables);
                } else {
                    Logs.write("Considering specified tables only in Primary DB 1 for compare");
                    tableNames = specifiedTables;
                }
            } else {
                Logs.write("Considering all the tables in Primary DB 1 for compare");
            }

            vendor1 = dbutil.vendor;
            dbname1 = dbutil.dbname;
            //System.out.println("Vendor for DB OBject 1 is " + vendor1);
            Logs.write("Vendor for DB OBject 1 is " + vendor1);
            dbutil.setDbInfo(new File("secondarydb.inf"));
            vendor2 = dbutil.vendor;
            dbname2 = dbutil.dbname;
            //System.out.println("Vendor for DB OBject 2 is " + vendor2);
            Logs.write("Vendor for DB OBject 2 is " + vendor2);

        } catch (ClassNotFoundException cnfe) {
            Logs.write("DataCheck Object - DB connection library not found", cnfe);
        } catch (SQLException sqle) {
            Logs.write("DataCheck Object - Error while retrieving table names", sqle);
        }

        int i = 0;
        int limit = tableNames.size();

        //dc.out.write("TABLE_NAME, " + vendor1 + "_" + dbname1 + ", " + vendor2 + "_" + dbname2 + ", results");
        //dc.out.write("\n");
        //LinkedList<String> skipTables = getTableNamesFromFile("skiptables.txt");
        //LinkedList<String> selectedTables = getTableNamesFromFile("selectedtables.txt");

        Enumeration enumerate = tableNames.elements();

        while (enumerate.hasMoreElements()) {
            Vector<String> dataRow = new Vector<String>();
            String val1 = null;
            String val2 = null;
            String table = (String) enumerate.nextElement();
            //dc.out.write(table);

//            if(skipTables.contains(table))
//            {
//            	Logs.write("Skipping table: " + table + " listed in skiptables list");
//            	continue;
//            }
//
//            if(selectedTables.size() != 0 & (!selectedTables.contains(table)))
//            {
//            	Logs.write("Ignoring table: " + table + " not listed in selectedtables list");
//            	continue;
//            }

            dataRow.add(table);
            // System.out.println("Processing table: " + table);
            Logs.write("Processing table: " + table);
            //dc.out.write(", ");

            if (vendor1.equalsIgnoreCase("mysql")) {
                objMy1 = new MySQLData();
                objMy1.setConnection("primarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getLastNRowsData(table, N, writeDataContents));
                    //val1 = MD5Util.getMD5FromStringBuffer(objMy1.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMy1.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val1);
                //dataRow.add(val1);
                objMy1.closeConnection();
                objMy1 = null;
            } else if (vendor1.equalsIgnoreCase("mssql")) {
                objMs1 = new MsSQLData();
                objMs1.setConnection("primarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getLastNRowsData(table, N, writeDataContents));
                    //val1 = MD5Util.getMD5FromStringBuffer(objMs1.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val1 = MD5Util.getMD5FromStringBuffer(objMs1.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val1);
                //dataRow.add(val1);
                objMs1.closeConnection();
                objMs1 = null;
            }

            // dc.out.write(", ");

            if (vendor2.equalsIgnoreCase("mysql")) {
                objMy2 = new MySQLData();
                objMy2.setConnection("secondarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getLastNRowsData(table, N, writeDataContents));
                    //val2 = MD5Util.getMD5FromStringBuffer(objMy2.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMy2.getRandomNthRowData(table, N, writeDataContents));
                }
                //dc.out.write(val2);
                //dataRow.add(val2);
                objMy2.closeConnection();
                objMy2 = null;
            } else if (vendor2.equalsIgnoreCase("mssql")) {
                objMs2 = new MsSQLData();
                objMs2.setConnection("secondarydb.inf");
                if (processType.equalsIgnoreCase("firstNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getFirstNRowsData(table, N, writeDataContents));
                } else if (processType.equalsIgnoreCase("lastNRows")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getLastNRowsData(table, N, writeDataContents));
                    //val2 = MD5Util.getMD5FromStringBuffer(objMs2.getReverseNRowsData(table, N));
                } else if (processType.equalsIgnoreCase("randomNthRow")) {
                    val2 = MD5Util.getMD5FromStringBuffer(objMs2.getRandomNthRowData(table, N, writeDataContents));
                }

                //dc.out.write(val2);
                //dataRow.add(val2);
                objMs2.closeConnection();
                objMs2 = null;
            }


            //dc.out.write(", ");
            if (val1.equals(val2)) {
                if (val1.equals("d41d8cd98f00b204e9800998ecf8427e")) {
                    //dc.out.write("Both Empty");
                    dataRow.add("Both Empty");
                } else {
                    //dc.out.write("Matching");
                    dataRow.add("Matching");
                }
            } else {
                if (val2.equals("6420397f8502ace0bacf868c5936c11f")) {
                    //dc.out.write("Not Found");
                    dataRow.add("Not Found");
                } else {
                    //dc.out.write("Data Difference");
                    dataRow.add("Data Difference");
                }
            }

            //dc.out.write("\n");
            //dc.out.flush();
            dataVector.add(dataRow);
            i++;
            DBInfo.updateProgressStatus(DBInfo.getProgressBar1(), i, limit);
            DBInfo.updateProgressStatus(DBInfo.getProgressBar2(), i, limit);
        }
        t.stop();
        SwingUtilities.invokeLater(new Runnable() {

            public void run() {
                new TableFromFilteredVector("Compare " + processType, dataVector, headVector, t);
            }
        });
        VectorUtil.storeVectorResultInCSVFile(headVector, dataVector, new File("Compare_" + processType + "_" + DBInfo.getBriefName() + ".csv"));
        DBInfo.resetProgressBars();
        //dc.out.close();

    }
}
