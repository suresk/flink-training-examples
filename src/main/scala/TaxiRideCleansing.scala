import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object TaxiRideCleansing {
  def main(args: Array[String]) {
    val maxDelay = 60
    val servingSpeed = 600
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val rides = env.addSource(new TaxiRideSource("/Users/suresk/dev/flink-training/data/nycTaxiRides.gz", maxDelay, servingSpeed))
    
    rides.filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
    
    rides.print()
    
    env.execute("Cleansing taxi rides")
  }
}