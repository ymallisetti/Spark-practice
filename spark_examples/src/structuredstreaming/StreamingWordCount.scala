package structuredstreaming
import util.SparkUtil._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils,ConsumerStrategies,LocationStrategies}

/**
 * this class acts as a consumer from the kafka topic,
 * kafka setup is local
 * The program will start running and listening to the streams only when
 * kafka server is started.
 */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupId = "GRP1"//since consumer is running inside of spark, any value will work ??
    val topics = "kafkawc"
    
    //val spark = getLocalSparkSession -- Need to check if only sparkconf is needed for connecting kafka??
    //val sparkConf = spark.sparkContext.getConf
    
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaWorkdCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val sc = ssc.sparkContext 
    sc.setLogLevel("OFF")
    
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream[String, String](
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )
    
    val line = messages.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordsCounts = words.map(x => (x,1)).reduceByKey(_+_)
    
    wordsCounts.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}