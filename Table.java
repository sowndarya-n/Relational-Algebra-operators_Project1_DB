
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * The Table class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key (the attributes forming). 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.TREE_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        return switch (mType) {
        case TREE_MAP    -> new TreeMap <> ();
//      case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
//      case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
        default          -> null;
        }; // switch
    } // makeMap

    /************************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1  the first array
     * @param arr2  the second array
     * @return  a wider array containing all the values from arr1 and arr2
     */
    public static <T> T [] concat (T [] arr1, T [] arr2)
    {
        T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
        arraycopy (arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();
    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        var attrs     = attributes.split (" ");
        var colDomain = extractDom (match (attrs), domain);
        var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        //Iterating through every row in the input table
        for (Comparable[] data : tuples)
        {
            // Creating an array to store the attributes that we project
            Comparable[] project_attr = new Comparable[attrs.length];
            //Looping through every attribute that is stored in attrs
            for(int i=0; i<attrs.length; i++)
            {
                //Finding index of the column that is projected 
                int proj_column = col(attrs[i]);
                //Storing the projected column values to the array we created
                project_attr[i] = data[proj_column];
            }
            //Adding the projected row to the table
            rows.add(project_attr);
        }
        return new Table (name + count++, attrs, colDomain, newKey, rows);
        } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given simple condition on attributes/constants
     * compared using an <op> ==, !=, <, <=, >, >=.
     *
     * #usage movie.select ("year == 1977")
     *
     * @param condition  the check condition as a string for tuples
     * @return  a table with tuples satisfying the condition
     */
    public Table select (String condition)
    {
        out.println ("RA> " + name + ".select (" + condition + ")");
        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
       // Extracting the attributes and the operator from the condition specified
        String[] split_condition = condition.split(" ");
        //Storing the operator that is extracted
        String extracted_operator = split_condition[1];
        //Storing the column position of the attribute
        int colPos = col(split_condition[0]);
        //Converting the third part of the condition to integer to perform numerical comparisons
        int operand = Integer.parseInt(split_condition[2]);
    
        // Iterate through each tuple of the movie table
        for (var m_t : tuples) {
            Comparable value = m_t[colPos];    
            // Defining a flag that is used to check the condition
            boolean match_flag = false;
            //Performing actions based on the operator that is extracted
            switch (extracted_operator) 
            {
                case "==":
                    //checks if value is equal to operand
                    match_flag = value.equals(operand); 
                    break;
                case "!=":
                    //checks if value is not equal to operand
                    match_flag = !value.equals(operand); 
                    break;
                case "<":
                    //checks if value is less than the operand
                    match_flag = value.compareTo(operand) < 0; 
                    break;
                case "<=":
                    //checks if value is less than or equal to the operand
                    match_flag = value.compareTo(operand) <= 0;  
                    break;
                case ">":
                   //checks if value is greater than the operand
                    match_flag = value.compareTo(operand) > 0;  
                    break;
                case ">=":
                    //checks if value is greater than or equal to the operand
                    match_flag = value.compareTo(operand) >= 0; 
                    break;
                default:
                    //If the operator is not in the specified format
                    out.println("Unsupported operator: " + extracted_operator);
            }
            // If the match is set true we add that particular tuple to the resultant table row
            if (match_flag) 
            {
                rows.add(m_t);
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
     } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.  INDEXED SELECT ALGORITHM.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D  - Project 2

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * **Author Sowndarya Nookala***
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        //Checking if two tables are compatabile for union operation
        if (! compatible (table2)) return null;
        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D

        // Adding all the tuples from first table
        rows.addAll(tuples);
        //Iterating through all the tuples in second table 
        for (var row : table2.tuples) 
        {
            //Checking if the tuple from second table is already stored in rows 
            if (!rows.contains(row)) 
            {  
                //If the current tuple of table2 is not already in table1 we add it
                rows.add(row);
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        // Looping to iterate over the tuples of first table 
        for (var frsttab_row : tuples)
        {
            //Condition to check if the tuples of table 1 are not in table 2
            if(table2.tuples.contains(frsttab_row))
            { 
                continue;
            }
            else
            {
                //if the condition is false, we add the tuples to the resultant rows variable created
                rows.add(frsttab_row);
            }
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by appending "2" to the end of any duplicate attribute name.  Implement using
     * a NESTED LOOP JOIN ALGORITHM.
     *
     * #usage movie.join ("studioName", "name", studio)
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");
        var rows    = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D 

        // Extracting the column positions for the join attributes of both tables
        int[] t_match = match(t_attrs);
        int [] u_match = table2.match(u_attrs);

        // Removing the disambiguate attribute names by appending 2 at the end
        String[] attr_table2 = table2.attribute;
        for (int i = 0; i < attr_table2.length; i++) {
            String attr_initial = attr_table2[i];
            String attr_after = attr_initial;

            // A condition to check if attribute name already exists in the first table's attributes
            if (Arrays.asList(attribute).contains(attr_after)) {
                // If there is an attribute with same name, we append "2" until the attribute becomes unique
                int append_val = 2;
                while (Arrays.asList(attribute).contains(attr_after)) {
                    attr_after = attr_initial + append_val;
                    append_val++;
                }
            }
            // Updating the final name of the attribute in table2
            attr_table2[i] = attr_after;
        }

        //Iterating through every row in table 1
        for(var table1_data : tuples)
        {
            //Extracting the values of join attributes of current tuple in table 1
            Comparable[] table1_values = extract(table1_data, t_attrs);
            //Iterating through every row in table 2
            for(var table2_data : table2.tuples)
            {
                //Extracting the values of join attributes of current tuple in table 2
                Comparable[] table2_values = table2.extract(table2_data, u_attrs);
                // Boolean flag that is used to check if the join condition is satisfied or not
                boolean flag = true;
                //Looping through the values of tables
                for(int i=0; i<table1_values.length;i++)
                {
                    //Checking if the values of both tables are equal or not
                    if(!table1_values[i].equals(table2_values[i]))
                    {
                        //If the values are not equal, the flag is set to false
                        flag = false;
                        break;
                    }
                }
                //If the flag is set to true
                if(flag)
                {
                    //We concatenate the tuples of tables 1 & 2 and store them in a new array   
                    Comparable[] result_data = concat(table1_data,table2_data);
                    // Adding the concatenated tuples to the rows of resultant table
                    rows.add(result_data);
                }
            }
        }
        return new Table (name + count++, concat (attribute, table2.attribute),concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing a "theta-join".  Tuples from both tables
     * are compared attribute1 <op> attribute2.  Disambiguate attribute names by appending "2"
     * to the end of any duplicate attribute name.  Implement using a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioName == name", studio)
     *
     * @param condition  the theta join condition
     * @param table2     the rhs table in the join operation
     * @return  a table with tuples satisfying the condition
     */
    public Table join (String condition, Table table2)
    {
        out.println ("RA> " + name + ".join (" + condition + ", " + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D
        // Extracting the attributes and the operator from the condition specified
        String[] condition_split = condition.split(" ");
        //Storing the operator that is extracted
        String operator_split = condition_split[1];
        //The col position of LHS attribute
        int lhs_op = col(condition_split[0]);
        //The col position of RHS attribute
        int rhs_op = table2.col(condition_split[2]); 
        
        // Removing the disambiguate attribute names by appending 2 at the end
        String[] attr_table2 = table2.attribute;
        for (int i = 0; i < attr_table2.length; i++) {
            String attr_initial = attr_table2[i];
            String attr_after = attr_initial;

            // A condition to check if attribute name already exists in the first table's attributes
            if (Arrays.asList(attribute).contains(attr_after)) {
                // If there is an attribute with same name, we append "2" until the attribute becomes unique
                int append_val = 2;
                while (Arrays.asList(attribute).contains(attr_after)) {
                    attr_after = attr_initial + append_val;
                    append_val++;
                }
            }
            // Updating the final name of the attribute in table2
            attr_table2[i] = attr_after;
        }

        // Iterating through every tuple in the first table
        for (var t_row : tuples) 
        {
        // Extracting values of join attributes from the current tuple
        Comparable t_values = t_row[lhs_op];
        // Iterating through each tuple in the table2
        for (var u_row : table2.tuples) 
        {
            // Extract the values of the join attributes from the table2 tuple
            Comparable u_values = u_row[rhs_op];   
            // Defining a flag that is used to check the condition
            boolean match = false;
            //Performing actions based on the operator that is extracted
            switch (operator_split) 
            {
                case "==":
                   //checks if value is equal to operand
                    match = t_values.equals(u_values);   
                    break;
                case "!=":
                    //checks if value is not equal to operand
                    match = !t_values.equals(u_values);   
                    break;
                case "<":
                    //checks if value is less than the operand
                    match = t_values.compareTo(u_values) < 0; 
                    break;
                case "<=":
                    //checks if value is less than or equal to the operand
                    match = t_values.compareTo(u_values) <= 0; 
                    break;
                case ">":
                    //checks if value is greater than the operand
                    match = t_values.compareTo(u_values) > 0; 
                    break;
                case ">=":
                    //checks if value is greater than or equal to the operand
                    match = t_values.compareTo(u_values) >= 0; 
                    break;
                default:
                    //checks and prints if the operator type is not defined
                    out.println("Unsupported operator: " + operator_split);
            }
            // If the match is set true we add that particular tuple to the resultant table row
            if (match) 
            {
                Comparable[] combined = concat(t_row, u_row);
                rows.add(combined);
            }
        }
    }
    return new Table (name + count++, concat (attribute, table2.attribute),concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above equi-join,
     * but implemented using an INDEXED JOIN ALGORITHM.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        //  T O   B E   I M P L E M E N T E D  - Project 2

        return null;

    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {

        //  D O   N O T   I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D 

        // Extracting common attributes between the two tables
        int commonAttrrs_true=0;
        List<String> commonAttrs = new ArrayList<>();
        for (String attr : attribute) 
        {
            // Checking if the attribute of table 2 has table 1 attribute
            if (Arrays.asList(table2.attribute).contains(attr)) 
            {
                //If yes, the variable is set to 1 indicating there is a match
                commonAttrrs_true=1;
                //Adding the common attributes found to the array list we created
                commonAttrs.add(attr);
            }
        }

        // Iterate through each tuple in first table
        for (var t_row : tuples) {
            // Extracting the common attributes values from the tuple that is being iterated.
            Comparable[] t_values = extract(t_row, commonAttrs.toArray(new String[0]));
            // Iterating over each tuple in the table2
            for (var u_row : table2.tuples) {
                // Extracting the common attributes values from the tuples of table2 
                Comparable[] u_values = table2.extract(u_row, commonAttrs.toArray(new String[0]));
                
                // We now check if these values of common attributes match
                int values_check=1;
                for (int i = 0; i < t_values.length; i++) 
                {
                    //Checking if the values of both common attributes match
                    if (!t_values[i].equals(u_values[i])) 
                    {
                        // If they do not match, the value is set to zero. 
                        values_check=0;
                        break;
                    }
                }
                //If there are common attributes
                if(commonAttrrs_true==1)
                {
                    //If the values of these common attributes match
                    if (values_check==1) 
                    {
                    //We add the tuple from first table to the result
                    rows.add(t_row);
                    }
                }
                //If there are no common attributes
                else
                {
                    //If the values have a match
                    if (values_check==1) 
                    {
                        // We combine the tuples that are matched by eliminating the duplicate columns    
                        Comparable[] combined = concat(t_row, u_row);
                        // Adding the combined tuples to the result
                        rows.add(combined);
                    }
                }
            }
        }
        // FIX - eliminate duplicate columns
       // Create a list to hold the unique attributes for the new table
        List<String> uniqueAttrs = new ArrayList<>(Arrays.asList(attribute));
        // Remove duplicate attributes from the list
        for (String attr : table2.attribute) 
        {
            //If the new tuple from table 2 is not already in the unique list, we add it.
            if (!uniqueAttrs.contains(attr)) 
            {
                uniqueAttrs.add(attr);
            }
        }
        //Checking if there are common attributes or not.
        if (commonAttrrs_true==1)
        {
            // If yes, we create a new table with the unique attributes
            return new Table (name + count++, uniqueAttrs.toArray(new String[0]),domain, key, rows);
        }
        else
        {
            // Else, we concat the attributes 
            return new Table (name + count++, concat (attribute, table2.attribute), concat (domain, table2.domain), key);
        }
    } // join


    /************************************************************************************
     * Return the column position for the given attribute name or -1 if not found.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (var i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;       // -1 => not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        out.print ("| ");
        for (var a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        for (var tup : tuples) {
            out.print ("| ");
            for (var attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in array) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a array of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;      // change once implemented
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos  the column positions to extract.
     * @param group   where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

} // Table class

