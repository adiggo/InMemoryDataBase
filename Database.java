package Thumbtack;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by Xiaoping li on 12/15/14.
 */
public class Database {

    Stack<HashMap<String, String>> transactionHistory;
    HashMap<String, String> database;
    HashMap<String, Integer> recordsOfValue;
    static String[] commands;
    public Database() {
        transactionHistory = new Stack<HashMap<String, String>>();
        database = new HashMap<String, String>();
        recordsOfValue = new HashMap<String, Integer>();
        commands = new String[]{"BEGIN", "ROLLBACK", "COMMIT", "SET", "GET", "UNSET", "NUMEQUALTO"};
    }

    /**
     * this is a helper method for set method
     * @param name
     * @param val
     */
    public void setVal(String name, String val) {

        if (val != null) {
            if (!database.containsKey(name)) {
                int valOfKey = recordsOfValue.containsKey(val)? recordsOfValue.get(val) : 0;
                recordsOfValue.put(val, valOfKey + 1);
            }
            else if (database.containsKey(name)){
                String tmpVal = database.get(name);
                recordsOfValue.put(tmpVal, recordsOfValue.get(tmpVal)-1);
                if (recordsOfValue.containsKey(val))
                    recordsOfValue.put(val, recordsOfValue.get(name)+1);
                else
                    recordsOfValue.put(val, 1);
            }
            database.put(name, val);
        } else {
            if (database.containsKey(name)){

                recordsOfValue.put(database.get(name) ,recordsOfValue.get(database.get(name)) - 1);
            }
            database.remove(name);
        }
    }

    public void set(String key, String val) {
        if (transactionHistory.size() > 0) {
            if (database.containsKey(key) && !transactionHistory.peek().containsKey(key)) {
                transactionHistory.peek().put(key, database.get(key));
            }
            if (!database.containsKey(key)) {
                transactionHistory.peek().put(key, null);
            }
        }
        setVal(key, val);
    }

    public void get(String name) {
        if (database.containsKey(name))
            System.out.println(database.get(name));
        else
            System.out.println("NULL");
    }

    public void unset(String name) {
        if (database.containsKey(name)) {
            String val = database.get(name);
            database.remove(name);

            recordsOfValue.put(val, recordsOfValue.get(val) - 1);
        }
    }

    public void numequalto(String val) {
        if (recordsOfValue.containsKey(val))
            System.out.println(recordsOfValue.get(val));
        else
            System.out.println(0);
    }

    // push a new hashmap in the top of the stack
    public void begin() {
        transactionHistory.push(new HashMap<String, String>());
    }

    //new the transactionHistory
    public void commit() {
        this.transactionHistory = new Stack<HashMap<String, String>>();
    }

    //roll back the database
    public void rollback() {
        if (this.transactionHistory.size() > 0) {
            HashMap<String, String> temp = transactionHistory.pop();
            for (Map.Entry<String, String> entry : temp.entrySet()) {
                setVal(entry.getKey(), entry.getValue()); //also in this method will update the database map
            }
        } else {
            System.out.println("Invalid RollBack due to no available blocks");
        }
    }



    public static void main(String[] args) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        Database db = new Database();
        while((line = br.readLine()) != null){
            String[] arguments = line.split(" ");
            Method method;
            boolean operate = false;
            for (int i = 0; i < commands.length; i++){
                if (arguments[0].equals(commands[i])) {
                    operate = true;
                    break;
                }
            }
            if (operate == true) {
                if (arguments.length == 3) {
                    method = Database.class.getDeclaredMethod(arguments[0].toLowerCase(), arguments[1].getClass(), arguments[2].getClass());
                    method.invoke(db, arguments[1], arguments[2]);
                }
                if (arguments.length == 2) {
                    method = Database.class.getDeclaredMethod(arguments[0].toLowerCase(), arguments[1].getClass());
                    method.invoke(db, arguments[1]);
                }
                if (arguments.length == 1){
                    method = Database.class.getDeclaredMethod(arguments[0].toLowerCase());
                    method.invoke(db);
                }
            }else{
                System.out.println("Unknown Command");
            }
        }

    }


}
