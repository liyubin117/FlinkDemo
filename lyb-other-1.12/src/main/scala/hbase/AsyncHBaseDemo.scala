package hbase

import org.hbase.async._
import com.stumbleupon.async._

object AsyncHBaseDemo extends App {

    // This let's us pass inline functions to the deferred
    // object returned by most asynchbase methods
    implicit def conv[A, B](f: B ⇒ A): Callback[A, B] = {
        new Callback[A, B]() {
            def call(b: B) = f(b)
        }
    }

    // Specify your zookeeper connection below
    val client = new HBaseClient("fuxi-luoge-76")
    
    // Our dummy data
    val put = new PutRequest("lyb_testasync", "row", "f1", "column", "value")
    
    // asynchbase client methods
    client.ensureTableFamilyExists("lyb_testasync", "f1") addCallback {
        o: Object ⇒ println("Put Succeeded")
    } addErrback {
        e: Exception ⇒
            println("Table Assertion Error")
            println(e.getMessage())
    } join // wait for the assertion to continue

    client.put(put) addCallback { o: Object ⇒
        println("Inserted into Table")
    } addErrback { e: Exception ⇒
        println("Insertion error")
        println(e.getMessage())
    } join // need to wait, or else the app exits

    println("Hello World")
    
}