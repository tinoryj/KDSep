public class RocksDBInterface{
    static{
      com.github.fommil.jni.JniLoader.load("librocksdb-ycsb.so");
    }

    public static void main(String[] args){
      System.out.println("Ok!");
      RocksDBInterface.show();
    }

    public static native void show();
}