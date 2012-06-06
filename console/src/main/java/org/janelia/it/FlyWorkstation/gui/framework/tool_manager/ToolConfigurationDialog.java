package org.janelia.it.FlyWorkstation.gui.framework.tool_manager;

import org.janelia.it.FlyWorkstation.gui.framework.session_mgr.SessionMgr;
import org.janelia.it.FlyWorkstation.shared.util.Utils;


import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;
import java.util.prefs.BackingStoreException;

/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 5/5/11
 * Time: 10:40 AM
 */
public class ToolConfigurationDialog extends JDialog{


    private JFileChooser _toolFileChooser;
    private DefaultTableModel model;
    private ToolMgr toolMgr = SessionMgr.TOOL_MGR;
    private int selectedRow;
    private int selectedColumn;


    public ToolConfigurationDialog(final JFrame parentFrame) throws HeadlessException, BackingStoreException {
        super(parentFrame);
        getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));
        setSize(400, 400);
        setModal(true);
        model = new DefaultTableModel(){
                @Override
                public boolean isCellEditable(int row, int column) {
                //all cells false
                return false;
            }
        };
        model.addColumn("Tool");
        model.addColumn("Location");
        refreshTable();
        final JTable table = new JTable(model);
        table.setPreferredScrollableViewportSize(new Dimension(400, 100));
        table.setFillsViewportHeight(true);
        table.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                printDebugData(table);
                selectedRow = table.getSelectedRow();
                selectedColumn = table.getSelectedColumn();
            }
        });
        //Create the scroll pane and add the table to it.
        JScrollPane scrollPane = new JScrollPane(table);


        _toolFileChooser = new JFileChooser();

        JButton _addButton = new JButton("Add");
        _addButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent actionEvent) {
                int returnVal = _toolFileChooser.showOpenDialog(ToolConfigurationDialog.this);
                if (returnVal == JFileChooser.APPROVE_OPTION) {
                    File file = _toolFileChooser.getSelectedFile();
                    System.out.println("Opening: " + file.getName() + ".");
                }
                else {
                    System.out.println("Open command cancelled by user.");
                }

                if (_toolFileChooser.getSelectedFile().exists()) {

                    Tool toolTest = toolMgr.toolTreeMap.get("Tools." + SessionMgr.getUsername() + "." + _toolFileChooser.getSelectedFile().getName());
                    if (null == toolTest) {
                        toolMgr.addTool(new Tool(_toolFileChooser.getSelectedFile().getName().replaceAll(".exe", ""), _toolFileChooser.getSelectedFile().getAbsolutePath(), "brain.png", SessionMgr.getUsername()));
                        refreshTable();
                    }
                    else {
                        JOptionPane.showMessageDialog(ToolConfigurationDialog.this, "The tool has already been added.", "Tool Already Added", JOptionPane.WARNING_MESSAGE);
                    }
                    }


            }
        });

        JButton _editButton = new JButton("Edit");
        _editButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent actionEvent) {
                try {
                    Tool tool = new Tool("name", "path", "icon", "user");
                    String name = model.getValueAt(selectedRow, 0).toString();
                    String path = model.getValueAt(selectedRow, 1).toString();
                    Tool toolSystem = toolMgr.toolTreeMap.get("Tools.SYSTEM." + name);
                    Tool toolUser = toolMgr.toolTreeMap.get("Tools."+SessionMgr.getUsername()+"."+name);
                    if(null!=toolSystem){
                        tool = toolSystem;
                    }
                    else{
                        if(null!= toolUser){
                            tool = toolUser;
                        }
                    }
                    boolean isSystem = tool.getToolUser().equals("SYSTEM");
                    EditDialog editDialog = new EditDialog(parentFrame, name, path, isSystem);
                    model.setValueAt(editDialog.getNameText(), selectedRow, 0);
                    model.setValueAt(editDialog.getPathText(), selectedRow, 1);
                    if(isSystem){
                        toolMgr.addTool(new Tool(name, editDialog.getPathText(), tool.getToolIcon(), "SYSTEM"));
                    }
                    else {
                        toolMgr.removeTool(toolMgr.toolTreeMap.get("Tools."+SessionMgr.getUsername()+"."+name));
                        toolMgr.addTool(new Tool(editDialog.getNameText(), editDialog.getPathText(), tool.getToolIcon(), SessionMgr.getUsername()));
                    }


                }
                catch (BackingStoreException e) {
                    e.printStackTrace();
                }
            }
        });

        JButton _closeButton = new JButton("Close");
        _closeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent actionEvent) {
                ToolConfigurationDialog.this.setVisible(false);
            }
        });

        JButton _clearTool = new JButton("Delete");
        _clearTool.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                removeTool();
            }
        });

        getContentPane().add(new JLabel("Current Tools"));
        getContentPane().add(scrollPane);
        JPanel mainPanel = new JPanel();
        mainPanel.add(_addButton);
        mainPanel.add(_editButton);
        mainPanel.add(_clearTool);
        mainPanel.add(_closeButton);
        getContentPane().add(mainPanel);
        pack();
        Toolkit tk = Toolkit.getDefaultToolkit();
        Dimension screenSize = tk.getScreenSize();
        int screenHeight = screenSize.height;
        int screenWidth = screenSize.width;
        setLocation((screenWidth - getWidth()) / 2, (screenHeight - getHeight()) / 2);
        setVisible(true);
    }


    private void refreshTable() {

//            tmpPrefs.exportNode(System.out);
            int tmp = model.getRowCount();
            if (0 != tmp){
                for (int i = tmp; i > 0; i--){
                    model.removeRow(i - 1);
                }
            }

            for (int i = 0; i < toolMgr.toolTreeMap.keySet().size(); i++) {
                String tmpKey = toolMgr.toolTreeMap.keySet().toArray()[i].toString();
                model.addRow(new Object[]{tmpKey.replaceAll("Tools.","").replaceAll("SYSTEM.","").replaceAll(SessionMgr.getUsername()+".",""),
                        toolMgr.toolTreeMap.get(tmpKey).getToolPath()});
            }

//        catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    private void removeTool() {
        Tool tool = new Tool("name", "path", "icon", "user");
        String name = model.getValueAt(selectedRow, 0).toString();
        Tool toolSystem = toolMgr.toolTreeMap.get("Tools.SYSTEM." + name);
        Tool toolUser = toolMgr.toolTreeMap.get("Tools."+SessionMgr.getUsername()+"."+name);
        if(null!=toolSystem){
            tool = toolSystem;
        }
        else{
            if(null!= toolUser){
                tool = toolUser;
            }
        }



        toolMgr.removeTool(tool);
        if (!tool.getToolUser().equals("SYSTEM"))
            model.removeRow(selectedRow);
    }

    private void printDebugData(JTable table) {
        int numRows = table.getRowCount();
        int numCols = table.getColumnCount();
        javax.swing.table.TableModel model = table.getModel();

        System.out.println("Value of data: ");
        for (int i = 0; i < numRows; i++) {
            System.out.print("    row " + i + ":");
            for (int j = 0; j < numCols; j++) {
                System.out.print("  " + model.getValueAt(i, j));
            }
            System.out.println();
        }
        System.out.println("--------------------------");
    }

    public class EditDialog extends JDialog{
        private String nameText;
        private String pathText;
        private JTextField nameTextField;
        private JTextField pathTextField;
        private JFileChooser fileChooser;

        public EditDialog(JFrame aFrame, String name, String path, boolean isSystem) throws BackingStoreException {
            super(aFrame, "Edit", true);
            nameText = name;
            pathText = path;
            nameTextField = new JTextField(40);
            nameTextField.setText(name);
            if(isSystem){
                nameTextField.setEditable(false);
            }
            pathTextField = new JTextField(40);
            pathTextField.setText(path);
            getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.PAGE_AXIS));
            setSize(400, 400);

            fileChooser  = new JFileChooser();
            fileChooser.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                   pathTextField.setText(fileChooser.getSelectedFile().getAbsolutePath());
                }
            });

            JButton _filePathButton = null;
            try {
                _filePathButton = new JButton(Utils.getClasspathImage("magnifier.png"));
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            if (_filePathButton != null) {
                _filePathButton.addActionListener(new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        fileChooser.showOpenDialog(EditDialog.this);
                    }
                });
            }

            JButton _saveButton = new JButton("Save");
            _saveButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent actionEvent) {
                    if(!nameTextField.getText().isEmpty() && !pathTextField.getText().isEmpty()){
                        nameText = nameTextField.getText();
                        pathText = pathTextField.getText();
                        EditDialog.this.setVisible(false);
                    }
                    else{
                        JOptionPane.showMessageDialog(ToolConfigurationDialog.this, "Name and/or Path cannot be empty", "Edit Exception", JOptionPane.WARNING_MESSAGE);
                        EditDialog.this.setVisible(false);
                    }
                }
            });

            JButton _closeButton = new JButton("Close");
            _closeButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent actionEvent) {
                    EditDialog.this.setVisible(false);
                }
            });

            JPanel namePanel = new JPanel();
            namePanel.add(new JLabel("Name:"));
            namePanel.add(nameTextField);
            JPanel pathPanel = new JPanel();
            pathPanel.add(new JLabel("Path:"));
            pathPanel.add(pathTextField);
            pathPanel.add(_filePathButton);
            JPanel buttonPanel = new JPanel();
            buttonPanel.add(_saveButton);
            buttonPanel.add(_closeButton);
            getContentPane().add(namePanel);
            getContentPane().add(pathPanel);
            getContentPane().add(buttonPanel);
            pack();
            Toolkit tk = Toolkit.getDefaultToolkit();
            Dimension screenSize = tk.getScreenSize();
            int screenHeight = screenSize.height;
            int screenWidth = screenSize.width;
            setLocation((screenWidth - getWidth()) / 2, (screenHeight - getHeight()) / 2);
            setVisible(true);

        }

        public String getNameText(){
            return nameText;
        }

        public String getPathText(){
            return pathText;
        }
    }
}
