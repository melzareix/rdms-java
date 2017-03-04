package DBApp;

import java.io.*;

public class Table {

    int numPages;
    String name;
    String tablePath;
    String[] colNames;
    String[] colTypes;
    Page last;
    boolean open;

    //First Step: Constructing a table - You should initialize the variables given above -
    public Table(String name, String[] colNames, String[] colTypes) throws IOException {
        numPages = 0;
        this.name = name;
        this.colNames = colNames;
        this.colTypes = colTypes;
        this.tablePath = "tabledata/" + this.name + "/";
    }

    /**
     * Create New Page `.csv` File
     *
     * @param p: Page to add
     */

    public void addPage(Page p) throws IOException {

        /*
        * Create Table Directory If it doesn't exists.
        * */

        File tablePathDirectories = new File(tablePath);
        tablePathDirectories.mkdirs();

        /*
        * Create New Page File
        * */

        String pagePath = tablePath + (++numPages) + ".csv";
        PrintWriter printWriter = new PrintWriter(new FileOutputStream(pagePath), true);


        /*
        * Insert Records to page
        * */

        for (int i = 0; i < p.current; i++) {
            String record = String.join(",", p.data[i]) + "," + p.deleted[i];
            printWriter.println(record);
        }

        this.last = p;
        printWriter.close();

    }

    /**
     * Inserts a record of strings into the last page of the table if it is not full,
     * otherwise, it should add a new page into the folder and insert the record into it.
     * @param record: record to be inserted
     */

    public int insert(String[] record) throws ClassNotFoundException, IOException {

        if (last == null || !last.insert(record)) {

            Page newPage = new Page(colNames.length);
            newPage.insert(record);
            addPage(newPage);

        } else {

            /*
            * Read Page File
            * */

            String pagePath = tablePath + (numPages) + ".csv";

            FileWriter fileWriter = new FileWriter(pagePath, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            PrintWriter printWriter = new PrintWriter(bufferedWriter);

            /*
            * Create Comma separated row and write to file
            **/

            String newRecord = String.join(",", record) + ",false";
            printWriter.println(newRecord);
            printWriter.close();

        }
        return numPages;
    }

    /*
    * Test Table Insertion
    * */

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        String tName = "Student";
        String[] tColNames = {"ID", "Name", "GPA", "Age", "Year"};
        String[] tColTypes = {"int", "String", "double", "int", "int"};
        Table t = new Table(tName, tColNames, tColTypes);

        for (int i = 0; i < 300; i++) {
            String[] st = {"" + i, "Name" + i, "0." + i, "20", "3"};
            t.insert(st);
        }

        for (int i = 0; i < 300; i++) {
            String[] st = {"" + (i + 300), "Name" + (i + 300), "0." + (i + 300), "21", "4"};
            t.insert(st);
        }

        for (int i = 0; i < 300; i++) {
            String[] st = {"" + (i + 600), "Name" + (i + 600), "0." + (i + 600), "21", "3"};
            t.insert(st);
        }

        System.out.println(t);

    }
}