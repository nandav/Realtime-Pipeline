
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.sql
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

import kafka.serializer.StringDecoder // this has to come before streaming.kafka import
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import scala.collection.JavaConversions._

import java.util.Date

object KafkaSparkCassandra {

  def main(args: Array[String]) {

    // read the configuration file
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkCassandra")
    val kafka_broker = sparkConf.get("spark.kafka.broker");
    val kafka_topic = sparkConf.get("spark.kafka.topic");
   
   // Create spark streaming context with 10 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    val cassandra_host = sparkConf.get("spark.cassandra.connection.host")//cassandra host
    val cassandra_user = sparkConf.get("spark.cassandra.auth.username");
    val cassandra_pass = sparkConf.get("spark.cassandra.auth.password");

    
    val topicsSet = Set[String] (kafka_topic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafka_broker)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Create the processing logic
    // the spark processing isn't actually run until the streaming context is started
    // it will then run once for each batch interval
   
 
    // Get the lines, split them into words, count the words and print
    val wordCounts = messages.map(_._2) // split the message into lines
      .flatMap(_.split("\"|,|\\]|\\[")) //split into words
      .filter(w => (w.length() > 20))
      .filter(w => (w.length() < 40))
      .map (_.trim)
      .map(w => (w, 1L)).reduceByKey(_ + _) // count by word
      .map({case (w,c) => (w,new Date().getTime(),c)}) // add the current time to the tuple for saving

       wordCounts.print() 



    // connect directly to Cassandra from the driver to create the keyspace
    val cluster = Cluster.builder().addContactPoint(cassandra_host).withCredentials(cassandra_user, cassandra_pass).build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS bluedata WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS bluedata.consumer_complaints (word text, ts timestamp, count int, PRIMARY KEY(word, ts)) ")
    session.execute("TRUNCATE bluedata.consumer_complaints")
    session.close()


    // SomeColumns() is a helper class from the cassandra connector rdd to be mapped to the columns in the table
    wordCounts.saveToCassandra("bluedata", "consumer_complaints", SomeColumns("word" as "_1", "ts" as "_2", "count" as "_3"))


    // start processing
    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)

    // Get the results using spark SQL
    val sc = new SparkContext(sparkConf) // create a new spark core context
    val csc = new CassandraSQLContext(sc) // Get Cassandra SQL context
    val rdd_results = csc.sql("SELECT * from bluedata.consumer_complaints") // select the data from the table
    rdd_results.show(100) // print the first 100 records from the result
    sc.stop()
  }
}
