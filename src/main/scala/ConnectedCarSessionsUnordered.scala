import org.apache.flink.streaming.api.scala._
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{ConnectedCarEvent, GapSegment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Created by suresk on 4/14/17.
  */
object ConnectedCarSessionsUnordered {
  class ConnectedCarAssigner(watermarkMillis: Long) extends AssignerWithPunctuatedWatermarks[ConnectedCarEvent] {
    override def checkAndGetNextWatermark(lastElement: ConnectedCarEvent, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp - watermarkMillis)

    override def extractTimestamp(element: ConnectedCarEvent, previousElementTimestamp: Long): Long = element.timestamp
  }

  class CreateGapSegment extends WindowFunction[ConnectedCarEvent, GapSegment, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ConnectedCarEvent], out: Collector[GapSegment]): Unit = {
      out.collect(new GapSegment(input.asJava))
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val carData = env.readTextFile("/Users/suresk/dev/flink-training/data/carOutOfOrder.csv")

    carData
      .map(ConnectedCarEvent.fromString(_))
      .assignTimestampsAndWatermarks(new ConnectedCarAssigner(0))
      .keyBy(_.carId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(15)))
      .apply(new CreateGapSegment)
      .print()

    env.execute("Connected Car Sessions Unordered")
  }
}
