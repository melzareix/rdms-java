package DBApp;

import DBApp.Page;
import pagingConfiguration.configuration;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;

public class Page {

    String[][] data;
    boolean[] deleted;
    int current;

    //First Step: Constructing a page - You should initialize the variables given above -
    public Page(int noCol) {
        deleted = new boolean[configuration.pageSize];
        data = new String[configuration.pageSize][noCol];
        current = 0;
    }

    //Function1: A function that checks if the page is full
    public boolean isFull() {
        return current == configuration.pageSize;
    }

    //Function2: Inserting a record into the page
    public boolean insert(String[] val) {
        if (!isFull()) {
            data[current] = Arrays.copyOf(val, val.length - 1); // Data without deleted col
            deleted[current] = val[val.length - 1].equals("true"); // Set deleted col
            current++;
            return true;
        }
        return false;
    }

    /**
     * Save a page to the disk
     * @param pagePath: the path to save the file
     */

    public void saveData(String pagePath) throws FileNotFoundException {

        PrintWriter printWriter = new PrintWriter(new FileOutputStream(pagePath), true);
        for (int i = 0; i < current; i++) {
            String[] record = data[i];
            printWriter.println(String.join(",", record) + "," + deleted[i]);
        }

        printWriter.close();
    }

    //Function3: Inserting a set of records into a page - It will use Function2-
    public Page getData(int[] colNum) {
        return null;
    }

}
