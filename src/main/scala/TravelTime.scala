import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.{GeoUtils, TravelTimePredictionModel}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by suresk on 4/12/17.
  */
object TravelTime {

  def getConfiguration(): Configuration = {
    val config = new Configuration()
    config.setInteger("jobmanager.web.port", 48081)
    config.setBoolean("query.server.enable", true)
    config.setInteger("query.server.port", 48022)
    return config
  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(getConfiguration())
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(6, Time.seconds(10)))
    val speed = 600

    val source = new CheckpointedTaxiRideSource("/Users/suresk/dev/flink-training/data/nycTaxiRides.gz", speed)
    val rides = env.addSource(source)

    rides
      .filter(nycFilter _)
      .map(rideGrid _)
      // I'm doing this to only have a small number of keys so it is easier to meaningfully query
      .keyBy(_._1 % 4)
      .flatMap(new TravelTimePredictor).print()

    env.execute("travel time predictor")
  }

  def rideGrid(r: TaxiRide): (Int, TaxiRide) = (GeoUtils.mapToGridCell(r.endLon, r.endLat), r)

  def nycFilter(r: TaxiRide): Boolean = {
    return GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat)
  }

  class TravelTimePredictor extends RichFlatMapFunction[(Int, TaxiRide), (Long, Int, Int, Int)] {

    var modelState: ValueState[PredictionState] = _

    override def flatMap(in: (Int, TaxiRide), collector: Collector[(Long, Int, Int, Int)]): Unit = {
      val ride = in._2

      val predictionModel = modelState.value()

      val distance = GeoUtils.getEuclideanDistance(ride.endLon, ride.endLat, ride.startLon, ride.startLat)
      val direction = GeoUtils.getDirectionAngle(ride.startLon, ride.startLat, ride.endLon, ride.endLat)

      if (ride.isStart) {
        // This is the start of a ride, so we want to predict how long it will take
        val time = predictionModel.model.predictTravelTime(direction, distance)
        //collector.collect((ride.rideId, time))
        predictionModel.addPrediction(ride.rideId, time)
      } else {
        // ride ended, so update the model by calculating travel time in minutes
        val actualTime = (ride.endTime.getMillis - ride.startTime.getMillis) / 60000.0
        predictionModel.model.refineModel(direction, distance, actualTime)
        collector.collect(predictionModel.endTrip(ride.rideId, actualTime.round.toInt))
        modelState.update(predictionModel)
      }
    }

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[PredictionState](
        "predictionModel",
        TypeInformation.of(new TypeHint[PredictionState] {}),
        new PredictionState(new TravelTimePredictionModel)
      )

      descriptor.setQueryable("prediction-state")

      modelState = getRuntimeContext.getState(descriptor)
    }
  }
}
