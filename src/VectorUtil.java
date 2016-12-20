/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.JTextArea;

/**
 *
 * @author Raviraj
 */
public class VectorUtil {

    public VectorUtil() {
    }
//--------------------------------------------------------------------------------

    public static Vector<Object> filteredResultVector(Vector<Object> vObject, int indexOfSubVector, String filterValue) {
        Vector<Object> filteredResult = new Vector<Object>();

        Enumeration enumerate = vObject.elements();
        while (enumerate.hasMoreElements()) {
            Vector<String> filteredSubVector = new Vector<String>();
            Vector<String> temp = (Vector<String>) enumerate.nextElement();
            //Vector<String> temp = enumerate.nextElement();
            String tmp;
            try {
                tmp = temp.get(indexOfSubVector);
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                JOptionPane.showMessageDialog(null, "Given filter index is not in range");
                return filteredResult;
            }
            if (tmp.equalsIgnoreCase(filterValue)) {
                Enumeration enumerate1 = temp.elements();
                while (enumerate1.hasMoreElements()) {
                    filteredSubVector.add((String) enumerate1.nextElement());
                }
                filteredResult.add(filteredSubVector);
            }
        }

        return filteredResult;
    }

//--------------------------------------------------------------------------------
    public static Vector<Object> filteredRowMismatchResultVector(Vector<Object> vObject, int indexOfSubVector, String filterValue) {
        Vector<Object> filteredResult = new Vector<Object>();
        vObject = filteredResultVector(vObject, indexOfSubVector, filterValue);
        Enumeration enumerate = vObject.elements();
        while (enumerate.hasMoreElements()) {
            Vector<String> filteredSubVector = new Vector<String>();
            Vector<String> temp = (Vector<String>) enumerate.nextElement();
            //Vector<String> temp = enumerate.nextElement();
            String tmp;
            try {
                tmp = temp.get(indexOfSubVector - 1);
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                JOptionPane.showMessageDialog(null, "Given filter index is not in range");
                return filteredResult;
            }
            try {
                Integer.parseInt(tmp);

                Enumeration enumerate1 = temp.elements();
                while (enumerate1.hasMoreElements()) {
                    filteredSubVector.add((String) enumerate1.nextElement());
                }
                filteredResult.add(filteredSubVector);

            } catch (NumberFormatException nfe) {
            }
        }

        return filteredResult;
    }

//--------------------------------------------------------------------------------
    public static Vector<Object> doubleFilteredResultVector(Vector<Object> vObject, int indexOneOfSubVector, String filterValueOne, int indexTwoOfSubVector, String filterValueTwo) {
        Vector<Object> filteredResult = new Vector<Object>();

        filteredResult = filteredResultVector(vObject, indexOneOfSubVector, filterValueOne);
        filteredResult = filteredResultVector(filteredResult, indexTwoOfSubVector, filterValueTwo);

        return filteredResult;
    }

//--------------------------------------------------------------------------------
    public static void storeVectorResultInCSVFile(Vector<String> head, Vector<Object> vector, File csvFile) {
        try {
            String tmp = "";
            PrintStream out = new PrintStream(csvFile);
            Enumeration enumerate = head.elements();
            while (enumerate.hasMoreElements()) {
                tmp += enumerate.nextElement();
                if (enumerate.hasMoreElements()) {
                    tmp += ",";
                }
            }
            out.println(tmp);
            out.flush();

            enumerate = vector.elements();
            while (enumerate.hasMoreElements()) {
                Vector<String> subVector = (Vector<String>) enumerate.nextElement();
                Enumeration enumerate1 = subVector.elements();
                tmp = "";
                while (enumerate1.hasMoreElements()) {
                    tmp += enumerate1.nextElement();
                    if (enumerate1.hasMoreElements()) {
                        tmp += ",";
                    }
                }
                out.println(tmp);
                out.flush();
            }
            out.close();
        } catch (FileNotFoundException fnfe) {
            JOptionPane.showMessageDialog(null, fnfe.getMessage());
            fnfe.printStackTrace();
            System.exit(0);
        }
    }

//--------------------------------------------------------------------------------
	/*public static void printVectorResultInConsole(Vector<Object> vector)
    {
    Enumeration enumerate = vector.elements();
    while(enumerate.hasMoreElements())
    {
    System.out.println(enumerate.nextElement());
    }
    }*/
//--------------------------------------------------------------------------------
    public static void printVectorResultInConsole(Vector<Object> vector) {
        Enumeration enumerate = vector.elements();
        while (enumerate.hasMoreElements()) {
            System.out.println(enumerate.nextElement());
        }
    }
//--------------------------------------------------------------------------------
    public static Vector<String> getListOfIgnoreTables(String fileName) {
        Vector<String> specifiedTables = new Vector<String>();
        File f = new File(fileName);
        if(f.exists())
        {
            try {
                Scanner in = new Scanner(f);
                while(in.hasNext())
                {
                    String temp = in.nextLine();
                    StringTokenizer token = new StringTokenizer(temp, ",;");
                    while(token.hasMoreTokens())
                    {
                        String tkn = token.nextToken().trim();
                        if(!tkn.equals(""))
                            specifiedTables.add(tkn);
                    }
                }
            } catch (FileNotFoundException fnfe) {
                Logs.write("Error: Couldn't read file " + fileName, fnfe);
            }
        }
        return specifiedTables;
    }
}
