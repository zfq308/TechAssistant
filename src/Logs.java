/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.Date;
import javax.swing.JOptionPane;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;

/**
 *
 * @author Raviraj
 */
public class Logs {

    public static PrintStream logStream, spLogStream;
    public static File logFile, spLogFile;

    static {
        File logDir = new File("logs");
        if (!logDir.exists()) {
            logDir.mkdir();
        }


        GregorianCalendar date = new GregorianCalendar();
        int month = Integer.valueOf(date.get(Calendar.MONTH)) + 1;
        String filePath = logDir.getAbsolutePath() + "\\eCW_Cools_" + date.get(Calendar.YEAR) + "-" + month + "-" + date.get(Calendar.DATE) + "_" + date.get(Calendar.HOUR) + "_" + date.get(Calendar.MINUTE) + "_" + date.get(Calendar.SECOND) + ".txt";
        logFile = new File(filePath);
        try {
            logStream = new PrintStream(logFile);
        } catch (FileNotFoundException fnfe) {
            JOptionPane.showMessageDialog(null, "Couldn't create log file" + fnfe.getMessage());
        }
    }

    public static void write(String message, Exception e) {
        logStream.println(new Date().toString() + ": " + message);
        logStream.println(e.getMessage());
        StackTraceElement ste[] = e.getStackTrace();
        for (StackTraceElement s : ste) {
            //logStream.print("\tline " + s.getLineNumber());
            //logStream.print(", class " + s.getClassName());
            //logStream.print(", method " + s.getMethodName());
            //logStream.println(", " + s.toString());
            logStream.println("\t\t" + s.toString());
        }
    }

    public static void write(String message) {
        logStream.println(new Date().toString() + ": " + message);
    }

    public static void write(String file, String data) {
        File logDir = new File("logs");
        RandomAccessFile out;
        if (!logDir.exists()) {
            logDir.mkdir();
        }

        String filePath = logDir.getAbsolutePath() + "\\" + file + "_log.txt";
        try {
            out = new RandomAccessFile(file, "rw");
            out.seek(out.length());
            out.writeBytes(new Date().toString() + ": " + data);
            out.writeBytes("\n");
            out.close();
        } catch (FileNotFoundException fnfe) {
            JOptionPane.showMessageDialog(null, "Couldn't create log file, " + file + "_log.txt - " + fnfe.getMessage());
        } catch (IOException ioe) {
            JOptionPane.showMessageDialog(null, "Couldn't write to log file, " + file + "_log.txt - " + ioe.getMessage());
        }

    }
}
