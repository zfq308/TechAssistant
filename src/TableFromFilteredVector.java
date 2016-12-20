/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.awt.BorderLayout;
import java.util.Vector;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

/**
 *
 * @author Raviraj
 */
public class TableFromFilteredVector extends JFrame {

    public TableFromFilteredVector(String title, Vector<Object> dataVector, Vector<String> headVector) {
        super(title);
        setLayout(new BorderLayout());
        JPanel buttonPanel = new JPanel();
        JTable table = new JTable(dataVector, headVector);
        JScrollPane scroll = new JScrollPane(table);

        add(new JLabel("Total : " + table.getRowCount() + " results found"), BorderLayout.SOUTH);
        add(table.getTableHeader(), BorderLayout.NORTH);
        add(scroll, BorderLayout.CENTER);


        pack();
        setVisible(true);
    }

    public TableFromFilteredVector(String title, Vector<Object> dataVector, Vector<String> headVector, Timer t) {
        super(title);
        setLayout(new BorderLayout());
        JPanel buttonPanel = new JPanel();
        JTable table = new JTable(dataVector, headVector);
        JScrollPane scroll = new JScrollPane(table);

        add(new JLabel("Total : " + table.getRowCount() + " results found ----- time took " + t.processInSecs() + " s"), BorderLayout.SOUTH);

        add(table.getTableHeader(), BorderLayout.NORTH);
        add(scroll, BorderLayout.CENTER);


        pack();
        setVisible(true);
    }
}
