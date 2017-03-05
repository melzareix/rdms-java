package DBApp;

import pagingConfiguration.configuration;

import java.io.*;
import java.util.*;

public class Main {

    public void init() {

    }

    /**
     * A method to create a table/relation.
     *
     * @param strTableName:    name of table to be created
     * @param htblColNameType: a hashtable of column names and column types , for example ("ID", "java.lang.Integer")
     *                         where ID is key and the value is java.lang.Integer
     * @param strKeyColName:   the name of the key column, for example "ID"
     */

    private void createTable(String strTableName, Hashtable<String, String> htblColNameType, String strKeyColName)
            throws DBException, IOException {

        /*
        * Add Entry to Metadata File
        * Table Name, Column Name, Column Type, Key, Indexed
        * */

        FileWriter fileWriter = new FileWriter(configuration.metadataFile, true);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        PrintWriter printWriter = new PrintWriter(bufferedWriter);

        /*
        * Java 8 Enhanced Loop
        * https://www.mkyong.com/java8/java-8-foreach-examples/
        * */

        htblColNameType.forEach((key, value) -> {
            Boolean isPrimary = strKeyColName.equals(key);

            /*
            * Return Comma separated file from array.
            * http://stackoverflow.com/questions/1978933/
            * */

            String columnSchema = String
                    .join(",", new String[]{strTableName, key, value, isPrimary.toString(), "false"});
            printWriter.println(columnSchema);

        });

        printWriter.close();

    }


    /**
     * A method to return the metadata for an existing table.
     *
     * @param strTableName: name of table to retrieve the metadata.
     * @return Hashtable mapping each column to its data.
     */

    private Hashtable<String, String> loadTableMetadata(String strTableName) throws IOException {
        Hashtable<String, String> htblColNameType = new Hashtable<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(configuration.metadataFile));

        String currentLine, colName, colType;
        String[] currentLineSeparated;
        boolean found = false;

        /*
        * Iterate Over the file, Get Column(s) Names and Type(s)
        * */

        while (((currentLine = bufferedReader.readLine()) != null)) {
            currentLineSeparated = currentLine.split(",");

            if (!currentLineSeparated[0].equals(strTableName)) {
                if (found) break; // Skip rest of the file
                continue;
            }

            found = true;

            colName = currentLineSeparated[1];
            colType = currentLineSeparated[2];

            htblColNameType.put(colName, colType);
        }

        return htblColNameType;
    }

    /**
     * A method to load an existing table.
     *
     * @param strTableName: name of table to retrieve the metadata.
     * @return The desired table as Table Object.
     */

    private Table loadTable(String strTableName) throws IOException {

        Hashtable<String, String> metadata = this.loadTableMetadata(strTableName);

        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<String> colTypes = new ArrayList<>();

        /*
        * Convert Hashmap to Array.
        * */

        metadata.forEach((key, value) -> {
            colNames.add(key);
            colTypes.add(value);
        });

        /*
        * Create In-Memory Table and Insert Record.
        * */

        return new Table(strTableName, colNames.toArray(new String[colNames.size()])
                , colTypes.toArray(new String[colTypes.size()]));
    }

    /**
     * A method to insert a tuple into an existing table
     *
     * @param strTableName:     name of table to insert into
     * @param htblColNameValue: a hashtable of column names and values for each column, for example, ("ID", "50011") where ID is
     *                          the name of the column and 50011 is the value to be inserted.
     */

    private void insertIntoTable(String strTableName, Hashtable<String, String> htblColNameValue)
            throws DBException, IOException, ClassNotFoundException {

        Table table = this.loadTable(strTableName);

        String[] record = new String[htblColNameValue.size()];
        for (int i = 0; i < table.colNames.length; i++) {
            record[i] = htblColNameValue.get(table.colNames[i]);
        }

        table.insert(record);
    }

    /**
     * A method to delete one or more tuple(s) from an existing table
     *
     * @param strTableName:     name of table to delete from
     * @param htblColNameValue: a hashtable of column names and values for each column, for example, ("ID", "50011") where ID is
     *                          the name of the column and 50011 is the value to be used for identifying row to be deleted.
     * @param strOperator:      possible values are AND or OR to combine the keys in htblColNameValue
     */

    public void deleteFromTable(String strTableName, Hashtable<String, String> htblColNameValue, String strOperator)
            throws DBException, IOException, ClassNotFoundException {

        Table table = this.loadTable(strTableName);
        Page currentPage;

        boolean isAND = strOperator.equals("AND");

        for (int i = 0; i < table.numPages; i++) {
            currentPage = table.loadPage(i + 1); // Pages start from 1

            for (int k = 0; k < currentPage.current; k++) {
                boolean deleteRecord = false;
                String[] record = currentPage.data[k];

                for (int j = 0; j < record.length; j++) {
                    String colName = table.colNames[j];
                    String colValue = record[j];
                    String queryValue = htblColNameValue.get(colName);

                    if (queryValue == null) continue;

                    boolean matches = htblColNameValue.get(colName).equals(colValue);

                    /*
                    * Deletion Logic
                    * If found a match mark delete, If on OR quit
                    * If not matching, If AND mark false and quit.
                    * */

                    if (matches) {
                        deleteRecord = true;
                        if (!isAND) break;
                    } else {
                        if (isAND) {
                            deleteRecord = false;
                            break;
                        }
                    }


                }

                if (deleteRecord) {
                    currentPage.deleted[k] = true;
                }

                currentPage.saveData(table.tablePath + (i + 1) + ".csv");

            }

        }

    }

    public static void main(String[] strArgs) throws DBException, IOException, ClassNotFoundException {

        Main testImplemenationMain = new Main();

        /*
        * Create Cities Table
        * */

        Hashtable<String, String> citiesTableSchema = new Hashtable<>();
        citiesTableSchema.put("ID", Integer.class.getName());
        citiesTableSchema.put("Name", String.class.getName());
        citiesTableSchema.put("Governorate", String.class.getName());
        citiesTableSchema.put("Founding_Date", Date.class.getName());

        testImplemenationMain.createTable("Cities", citiesTableSchema, "ID");

        /*
        * Create Students Table
        * */

        Hashtable<String, String> studentTableSchema = new Hashtable<>();
        studentTableSchema.put("ID", Integer.class.getName());
        studentTableSchema.put("Name", String.class.getName());
        studentTableSchema.put("Class", String.class.getName());
        studentTableSchema.put("Join Date", Date.class.getName());

        testImplemenationMain.createTable("Students", studentTableSchema, "ID");

        /* ============================================================================================= */

        /*
        * Insert Record(s) Into Cities Table
        * */

        Hashtable<String, String> citiesFirstRecord = new Hashtable<>();

        citiesFirstRecord.put("ID", "1");
        citiesFirstRecord.put("Name", "Cairo");
        citiesFirstRecord.put("Governorate", "New Cairo");
        citiesFirstRecord.put("Founding_Date", new Date().toString());

        testImplemenationMain.insertIntoTable("Cities", citiesFirstRecord);
//
        Hashtable<String, String> citiesSecondRecord = new Hashtable<>();

        citiesSecondRecord.put("ID", "1");
        citiesSecondRecord.put("Name", "Balabizo");
        citiesSecondRecord.put("Governorate", "October");
        citiesSecondRecord.put("Founding_Date", new Date().toString());

        testImplemenationMain.insertIntoTable("Cities", citiesSecondRecord);

        /*
        * Insert Record(s) Into Students Table
        * */

        Hashtable<String, String> studentsFirstRecord = new Hashtable<>();

        studentsFirstRecord.put("ID", "1");
        studentsFirstRecord.put("Name", "Mohamed");
        studentsFirstRecord.put("Class", "2014");
        studentsFirstRecord.put("Join Date", new Date().toString());

        testImplemenationMain.insertIntoTable("Students", studentsFirstRecord);


        Hashtable<String, String> studentsSecondRecord = new Hashtable<>();

        studentsSecondRecord.put("ID", "2");
        studentsSecondRecord.put("Name", "Islam");
        studentsSecondRecord.put("Class", "2015");
        studentsSecondRecord.put("Join Date", new Date().toString());

        testImplemenationMain.insertIntoTable("Students", studentsSecondRecord);

//        ========================================================================================

        /*
        * Delete Records from Cities Table
        * */

        Hashtable<String, String> citiesSecondRecordDel = new Hashtable<>();
        citiesSecondRecordDel.put("Governorate", "New Cairo");
        citiesSecondRecordDel.put("Name", "Cairo");

        testImplemenationMain.deleteFromTable("Cities", citiesSecondRecordDel, "AND");


        /*
        * Delete Records from Students Table
        * */

        Hashtable<String, String> studentsRecordDel = new Hashtable<>();
        studentsRecordDel.put("Class", "2014");

        testImplemenationMain.deleteFromTable("Students", studentsRecordDel, "OR");

        //        ================================================================================================

        System.out.println("TESTING FINISHED!");
    }
}
