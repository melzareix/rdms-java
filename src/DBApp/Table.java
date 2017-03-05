package DBApp;

import java.io.*;
import java.util.Arrays;

@SuppressWarnings("ConstantConditions")
public class Table {

    int numPages;
    String name;
    String tablePath;
    String[] colNames;
    String[] colTypes;
    Page last;
    boolean open;

    public Table(String name, String[] colNames, String[] colTypes) throws IOException {
        this.name = name;
        this.colNames = colNames;
        this.colTypes = colTypes;
        this.tablePath = "tabledata/" + this.name + "/";
        this.loadLastPage();
    }

    /**
    * Load the last page to the memory
    * */

    private void loadLastPage() throws IOException {
        try {
            File tablePathDirectories = new File(tablePath);
            this.numPages = tablePathDirectories.listFiles().length;
            this.last = loadPage(numPages);
        }catch (Exception e){
            this.numPages = 0;
        }
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
     * Insert a record of strings into the last page of the table if it is not full,
     * otherwise, it should add a new page into the folder and insert the record into it.
     *
     * @param record: record to be inserted
     */

    public int insert(String[] record) throws ClassNotFoundException, IOException {

        record = Arrays.copyOf(record, record.length + 1);
        record[record.length - 1] = "false"; // Is Deleted Field

        if (last == null || !last.insert(record)) {
            Page newPage = new Page(colNames.length);
            newPage.insert(record);
            addPage(newPage);
        } else {

            /*
            * Read Page File
            * */

            String pagePath = tablePath + (numPages) + ".csv";
            last.saveData(pagePath);
        }

        return numPages;
    }

    /**
     * Load a page into the memory
     *
     * @param idx: the number of the page to load
     */

    public Page loadPage(int idx) throws IOException {
        Page currentPage = new Page(this.colNames.length);
        String pagePath = tablePath + (idx) + ".csv";

        BufferedReader bufferedReader = new BufferedReader(new FileReader(pagePath));

        String currentLine;
        String[] currentLineSeparated;

        while (((currentLine = bufferedReader.readLine()) != null)) {
            currentLineSeparated = currentLine.split(",");
            currentPage.insert(currentLineSeparated);
        }

        bufferedReader.close();

        return currentPage;
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