package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    public static class TableDesc implements Serializable {
        //private TupleDesc desc_;
        private String name_;
        private String pkeyFieldId_;
        private DbFile dbFile_;

        public TableDesc(DbFile dbfile,String name,String pkeyFieldId) {
            name_ = name;
            pkeyFieldId_ = pkeyFieldId;
            dbFile_ = dbfile;
        }

        public String getName() {
            return name_;
        }

        public String getpkFieldId() { //获取主键
            return pkeyFieldId_;
        }

        public DbFile getDbFile() {
            return  dbFile_;
        }
    }
    private ArrayList<Integer> tables_; //其中每一项的值为tableid
    private ArrayList<TableDesc> tableDescs_;
    private ArrayList<TupleDesc> tupleDescs_;
    private int tableNum_;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() { //这个时候内部全都是空的
        // some code goes here
        tables_ = new ArrayList<Integer>();
        tableDescs_ = new ArrayList<TableDesc>();
        tupleDescs_ = new ArrayList<TupleDesc>();
        tableNum_ = 0;
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    //dbFile也就是这个表所属的db文件
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        TableDesc tabledesc = new TableDesc(file,name,pkeyField);//new一个新的table
        TupleDesc tupledesc = file.getTupleDesc();
        Integer tableid = file.getId();

        tables_.add(tableid);
        tableDescs_.add(tabledesc);
        tupleDescs_.add(tupledesc);

        tableNum_ ++;
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = tableNum_ - 1; i >= 0; i --) {
            String tmpName = tableDescs_.get(i).name_;
            if ((tmpName == null && name == null) || tmpName.equals(name)) {
                return tables_.get(i);
            }
        }
        throw new NoSuchElementException(String.format("Not %s in tables from getTableId",name));
    }
    //需要区分的是,不要把tableid和id搞混了
    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        int i;
        for (i = tableNum_ - 1;i >= 0; i --) {
            int id = tables_.get(i);
            if (id == tableid) {
                break;
            }
        }
        if (i == tableNum_) { //确定是找不到的情况
            throw new NoSuchElementException(String.format("Not such %s tableid from getTupleDesc",tableid));
        }
        return tupleDescs_.get(i);
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        int i;
        for (i = tableNum_ - 1;i >= 0; i --) {
            int id = tables_.get(i);
            if (id == tableid) {
                break;
            }
        }
        if (i == tableNum_) { //确定是找不到的情况
            throw new NoSuchElementException(String.format("Not such %s tableid from getDatabaseFile",tableid));
        }
        return tableDescs_.get(i).dbFile_;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        int i;
        for (i = tableNum_ - 1; i >= 0; i --) {
            int id = tables_.get(i);
            if (id == tableid) {
                break;
            }
        }
        if (i == tableNum_) { //确定是找不到的情况
            throw new NoSuchElementException(String.format("Not such %s tableid from getDatabaseFile",tableid));
        }
        return tableDescs_.get(i).pkeyFieldId_;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return tables_.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        int i;
        for (i = tableNum_ - 1;i >= 0; i --) {
            int tmpid = tables_.get(i);
            if (tmpid == id) {
                break;
            }
        }
        if (i == tableNum_) { //确定是找不到的情况
            throw new NoSuchElementException(String.format("Not such %s tableid from getDatabaseFile",id));
        }
        return tableDescs_.get(i).name_; //得到表名
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        tables_.clear();
        tableDescs_.clear();
        tupleDescs_.clear();
        tableNum_ = 0;
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) { //从文件中读取
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

