/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

/**
 *
 * @author Raviraj
 */
public class CompareTwoDBRowCounts {

    static Vector<String> headVectorForRows;
    static Vector<Object> dataVectorForRows;

    public static void start(java.awt.event.ActionEvent evt) {
        final ActionEvent ae = evt;
        new Thread(new Runnable() {

            public void run() {


                //DatabaseUtil temp = new DatabaseUtil();
                if (ae.getActionCommand().equals("Generate Row Count Result")) {
                    generateResultForRows();
                    SwingUtilities.invokeLater(new Runnable() {

                        public void run() {
                            DBInfo.switchMismatchAndMissingRowsButtonOn(true);
                        }
                    });
                } else if (ae.getActionCommand().equals("Show Row Count Mismatch")) {
                    final Vector<Object> filteredDataVector = VectorUtil.filteredRowMismatchResultVector(dataVectorForRows, 3, "NO");
                    SwingUtilities.invokeLater(new Runnable() {

                        public void run() {
                            new TableFromFilteredVector("Filtered Result", filteredDataVector, headVectorForRows);
                        }
                    });
                    VectorUtil.storeVectorResultInCSVFile(headVectorForRows, filteredDataVector, new File("RowMismatchOnly_" + DBInfo.getBriefName() + ".csv"));
                } else if (ae.getActionCommand().equals("Show Missing Tables")) {
                    final Vector<Object> filteredDataVector = VectorUtil.filteredResultVector(dataVectorForRows, 2, "Not Found");
                    SwingUtilities.invokeLater(new Runnable() {

                        public void run() {
                            new TableFromFilteredVector("Filtered Result", filteredDataVector, headVectorForRows);
                        }
                    });
                    VectorUtil.storeVectorResultInCSVFile(headVectorForRows, filteredDataVector, new File("TableNotFoundOnly_" + DBInfo.getBriefName() + ".csv"));
                }

            }
        }).start();
    }

    public static void generateResultForRows() {
        try {
            DatabaseUtil db1 = new DatabaseUtil();
            db1.setDbInfo(new File("primarydb.inf"));
            Connection con1 = db1.getConnection();
            DatabaseUtil db2 = new DatabaseUtil();
            db2.setDbInfo(new File("secondarydb.inf"));
            Connection con2 = db2.getConnection();

            headVectorForRows = new Vector<String>();
            headVectorForRows.add("TABLE_NAME");
            headVectorForRows.add("ROW_COUNT_FOR_" + DBInfo.getDbName(1));
            headVectorForRows.add("ROW_COUNT_FOR_" + DBInfo.getDbName(2));
            headVectorForRows.add("SUCCESS");

            Timer t = new Timer();
            t.start();
            //dataVector = db1.getRowCountComparisonsForTwoDatabasesInVector(con1, db1, con2, db2);
            //dataVectorForRows = db1.getRowCountComparisonsForTwoDatabasesInVector(con1, db1, con2, db2, DBInfo.getProgressBar1(), DBInfo.getProgressBar2());            
            dataVectorForRows = db1.getRowCountComparisonsForTwoDatabasesInVector(con1, db1, con2, db2, DBInfo.isIgnoreTablesSelected(), DBInfo.getProgressBar1(), DBInfo.getProgressBar2());
            t.stop();
            new TableFromFilteredVector("Row Count Comparison For " + DBInfo.getDbName(1) + " and " + DBInfo.getDbName(2), dataVectorForRows, headVectorForRows, t);
            //new TableFromVector("Row Count Comparison For Two DB1 and DB2", dataVector, headVector);
            VectorUtil.storeVectorResultInCSVFile(headVectorForRows, dataVectorForRows, new File("RowComparison_" + DBInfo.getBriefName() + ".csv"));

            db1.closeConnection(con1);
            db2.closeConnection(con2);
        } catch (ClassNotFoundException cnfe) {
            JOptionPane.showMessageDialog(null, cnfe.getMessage());
            DBInfo.resetProgressBars();
        } catch (SQLException sqle) {
            JOptionPane.showMessageDialog(null, sqle.getMessage());
            Logs.write("generate result - row counts", sqle);
            //eCWHelp.resetProgressBars();
        }
    }
}
