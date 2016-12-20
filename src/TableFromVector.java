/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.awt.BorderLayout;
import java.util.Vector;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

/**
 *
 * @author Raviraj
 */
public class TableFromVector extends JFrame {

    public TableFromVector(String title, Vector<Object> dataVector, Vector<String> headVector) {
        super(title);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLayout(new BorderLayout());
        JPanel buttonPanel = new JPanel();
        JTable table = new JTable(dataVector, headVector);
        JScrollPane scroll = new JScrollPane(table);
        //	JButton filterButton = new JButton("Row Mismatch Only");
        //	filterButton.addActionListener(new CompareTwoDBRowCounts());
        //	filterButton.setActionCommand("rowMismatch");
        //	buttonPanel.add(filterButton);
        //	JButton filterTableNotFoundButton = new JButton("Table Not Found Only");
        //	filterTableNotFoundButton.addActionListener(new CompareTwoDBRowCounts());
        //	filterTableNotFoundButton.setActionCommand("tableNotFound");
        //	buttonPanel.add(filterTableNotFoundButton);
        //JButton filterRowMismatchButton = new JButton("Row Mismatch Only");
        //filterRowMismatchButton.addActionListener(new CompareTwoDBRowCounts());
        //filterRowMismatchButton.setActionCommand("rowMismatch");
        //buttonPanel.add(filterRowMismatchButton);

        //	add(buttonPanel, BorderLayout.SOUTH);
        add(table.getTableHeader(), BorderLayout.NORTH);
        add(scroll, BorderLayout.CENTER);


        pack();
        setVisible(true);
    }
}
