import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel

import scala.collection.mutable

class PredictionState(@transient var model: TravelTimePredictionModel,
                      var trips: mutable.Map[Long, Int] = new mutable.HashMap,
                      var predictions: Int = 0,
                      var totalError: Int = 0,
                      var maxError: Int = 0,
                      var minError: Int = 0,
                      var averageError: Double = 0.0) {

  def this() {
    this(new TravelTimePredictionModel)
  }

  def addPrediction(tripId: Long, prediction: Int) = {
    if (prediction > -1) {
      trips.put(tripId, prediction)
    }
  }

  def endTrip(tripId: Long, actualTime: Int): (Long, Int, Int, Int) = {
    trips.get(tripId) match {
      case Some(prediction) => {
        val delta = math.abs(actualTime - prediction)
        predictions += 1
        totalError += delta

        if (delta > maxError) {
          maxError = delta
        }

        if (delta < minError) {
          minError = delta
        }

        averageError = totalError / predictions
        (tripId, prediction, actualTime, delta)
      }
      case None => (tripId, -1, actualTime, -1)
    }
  }
}