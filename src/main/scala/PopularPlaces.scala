import java.lang.Iterable

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import org.apache.flink.streaming.api.scala._
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

object PopularPlaces {

  class PopularPlaceWindowFunction(threshold: Int) extends WindowFunction[TaxiRide, (Float, Float, Long, Boolean, Int), (Boolean, Int), TimeWindow] {
    override def apply(key: (Boolean, Int), window: TimeWindow, input: scala.Iterable[TaxiRide], out: Collector[(Float, Float, Long, Boolean, Int)]): Unit = {
      if (input.size > threshold) {
        val lon = GeoUtils.getGridCellCenterLon(key._2)
        val lat = GeoUtils.getGridCellCenterLat(key._2)
        out.collect((lon, lat, window.maxTimestamp(), key._1, input.size))
      }
    }
  }

  def main(args: Array[String]) {
    val maxDelay = 60
    val servingSpeed = 600
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val rides = env.addSource(new TaxiRideSource("/Users/suresk/dev/flink-training/data/nycTaxiRides.gz", maxDelay, servingSpeed))
    
    rides.filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    rides.keyBy((t: TaxiRide) => {
      val rideCell = if(t.isStart) GeoUtils.mapToGridCell(t.startLon, t.startLat) else GeoUtils.mapToGridCell(t.endLon, t.endLat)
      (t.isStart, rideCell)
    }).timeWindow(Time.minutes(15), Time.minutes(5))
      .apply(new PopularPlaceWindowFunction(20))
      .print()

    env.execute("Finding popular places")
  }
}