import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.api.scala._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

/**
  * Created by suresk on 4/13/17.
  */
object QueryableStateClient {

  def main(args: Array[String]): Unit = {
    val key: Int = 1
    val descriptor = "prediction-state"
    val jobId = JobID.fromHexString("0d233b1316fdef00e7c6ded479d50d04")

    val config = new Configuration()
    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost")
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 48022)

    val client = new QueryableStateClient(config)

    val keySerializer = TypeInformation.of(new TypeHint[Int]() {}).createSerializer(new ExecutionConfig())

    val valueSerializer = TypeInformation.of(new TypeHint[PredictionState]() {}).createSerializer(new ExecutionConfig())

    val serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        key, keySerializer,
        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE)

    val serializedResult = client.getKvState(jobId, descriptor, key.hashCode(), serializedKey)

    val duration = new FiniteDuration(1, TimeUnit.SECONDS)
    val serializedValue = Await.result(serializedResult, duration)
    val state =
      KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer)

    print("Got state: ")
  }

}
