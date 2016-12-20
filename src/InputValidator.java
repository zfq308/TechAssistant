/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.awt.Color;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import javax.swing.JPasswordField;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;

/**
 *
 * @author Raviraj
 */
public class InputValidator {

    public static boolean isFilled(JTextField tField) {
        String text = tField.getText();

        if (text.equals("")) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isValidDomainUser(JTextField tField) {
        String text = tField.getText();

        try {
            if (!(text.indexOf("\\") > 0)) {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }
        } catch (StringIndexOutOfBoundsException siobe) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }

        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isFilledPassword(JPasswordField tField) {
        String text = new String(tField.getPassword());

        if (text.equals("")) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean samePassword(JPasswordField tField, JPasswordField tField2) {
        String text = new String(tField.getPassword());
        String text2 = new String(tField2.getPassword());

        if (!text.equals(text2)) {
            tField.setBackground(new Color(255, 255, 128));
            tField2.setBackground(new Color(255, 255, 128));
            return false;
        }
        tField.setBackground(Color.white);
        tField2.setBackground(Color.white);
        return true;
    }

    public static boolean isFilledArea(JTextArea tField) {
        String text = tField.getText();

        if (text.equals("")) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isText(JTextField tField) {
        String text = tField.getText();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (!Character.isLetter(c)) {
                if (!Character.isWhitespace(c)) {
                    tField.setBackground(new Color(255, 155, 155));
                    return false;
                }
            }
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isPhoneNumber(JTextField tField) {
        String text = tField.getText();
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isDigit(text.charAt(i))) {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }
            if (text.length() > 10) {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }

        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isPincode(JTextField tField) {
        String text = tField.getText();
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isDigit(text.charAt(i))) {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }
            if (text.length() != 6) {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }

        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isDate(JTextField tField) {
        //FORMAT for date is YYYY-MM-DD
        String text = tField.getText();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        if (text.trim().length() != dateFormat.toPattern().length()) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        dateFormat.setLenient(false);
        try {
            //parse the inDate parameter
            dateFormat.parse(text.trim());
        } catch (ParseException pe) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean isTime(JTextField tField) {
        //FORMAT for time is HH:MM:SS
        String text = tField.getText();

        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
        try {
            format.parse(text);
        } catch (ParseException e) {
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

    public static boolean hasRow(JTable table) {
        if (table.getRowCount() == 0) {
            return false;
        }
        return true;
    }

    public static boolean isEmail(JTextField tField) {
        String text = tField.getText();
        int i = text.indexOf("@");
        if (i > -1) {
            int j = text.indexOf(".", i);
            if (j > -1) {
                tField.setBackground(Color.white);
                return true;
            } else {
                tField.setBackground(new Color(255, 155, 155));
                return false;
            }
        }
        tField.setBackground(new Color(255, 155, 155));
        return false;
    }
/**
    public static boolean isIPReachable(JTextField tField) {
        String text = tField.getText();
        InetAddress address = null;
        try {
            if (confirmIPorName(tField).equals("Name")) {
                address = InetAddress.getByName(text);
            } else {
                address = InetAddress.getByAddress(text.getBytes());
            }
        } catch (UnknownHostException uhe) {
            Logs.write("Error ", uhe);
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

     public static String confirmIPorName(JTextField tField) {
        String confirm = "IP";
        String text = tField.getText();
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isDigit(text.charAt(i))) {
                if (!(text.charAt(i) == '.')) {
                    confirm = "Name";
                }
            }
        }
        return confirm;
    }

    public static byte[] getBytesFromIP(String ip) throws NumberFormatException
    {
    	byte b[] = new byte[4];
    	int dot = ip.indexOf(".");
    	System.out.println(ip);
    	String tmp = ip.substring(0, dot);
	   	System.out.println("b1 " + tmp);
    	b[0] = Byte.parseByte(tmp);
    	ip = ip.substring(dot + 1);
    	System.out.println(ip);
    	dot = ip.indexOf(".");
    	tmp = ip.substring(0, dot);
    	System.out.println("b2 " + tmp);
   		b[1] = Byte.parseByte(tmp);
    	ip = ip.substring(dot + 1);
    	dot = ip.indexOf(".");
    	tmp = ip.substring(0, dot);
    	System.out.println("b3 " + tmp);
    	b[2] = Byte.parseByte(tmp);
    	ip = ip.substring(dot + 1);
    	tmp = ip;
    	System.out.println("b4 " + tmp);
    	b[3] = Byte.parseByte(tmp);

    	return b;

    }

    */

    public static boolean isIPReachable(JTextField tField) {
        String text = tField.getText();
        try
        {
        	InetAddress address = InetAddress.getByName(text);

        } catch (UnknownHostException uhe) {
            Logs.write("Error ", uhe);
            tField.setBackground(new Color(255, 155, 155));
            return false;
        }
        tField.setBackground(Color.white);
        return true;
    }

}
