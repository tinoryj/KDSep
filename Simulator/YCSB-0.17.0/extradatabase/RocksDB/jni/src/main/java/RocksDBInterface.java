// package extradatabase.RocksDB;

public class RocksDBInterface{
    static{
      com.github.fommil.jni.JniLoader.load("libRocksDBInterface.so");
      // com.github.fommil.jni.JniLoader.load("librocksdb.so");s
    }

    public static void main(String[] args){
      System.out.println("Ok!");
      RocksDBInterface.show();
      RocksDBInterface.initDB("test-DB");
    }

    public static native void show();
    public static native boolean initDB(String databasePath);
}