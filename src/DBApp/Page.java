package DBApp;

import DBApp.Page;
import pagingConfiguration.configuration;

public class Page {

    String[][] data;
    boolean[] deleted;
    int current;
    private static configuration pagingConfiguration;

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
            data[current++] = val;
            return true;
        }
        return false;
    }

    //Function3: Inserting a set of records into a page - It will use Function2-
    public Page getData(int[] colNum) {
        return null;
    }

}
