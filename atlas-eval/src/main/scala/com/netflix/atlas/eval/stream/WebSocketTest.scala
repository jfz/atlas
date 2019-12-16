/*
 * Copyright 2014-2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2014-2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.Date

import akka.actor.ActorSystem
import akka.Done
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.ThrottleMode
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._

import scala.concurrent.duration._
import scala.concurrent.Future

object WebSocketTest {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    println(new Date)
    Source
      .repeat("a")
      .throttle(1, 10.milliseconds,1, ThrottleMode.Shaping)
      .to(Sink.foreach { msg =>
        Thread.sleep(1000)
        println(msg)
      }).run()
    Thread.sleep(100000)
  }
//    implicit val system = ActorSystem()
//    implicit val materializer = ActorMaterializer()
//    import system.dispatcher
//
//    // Future[Done] is the materialized value of Sink.foreach,
//    // emitted when the stream completes
//    val incoming: Sink[Message, Future[Done]] =
//      Sink.foreach[Message] {
//        case message: TextMessage.Strict =>
//          println(message.text)
//      }
//
//    // send this as a message over the WebSocket
//    val subScriptionStr = """[{"expression": "name,jvm.gc.pause,:eq,:sum", "step": 10000}]""";
//    val outgoing = Source
//      .repeat(TextMessage(subScriptionStr))
//      .throttle(1, 1.minute, 1, ThrottleMode.Shaping)
//
//    // flow to use (note: not re-usable!)
//    val webSocketFlow =
//      Http().webSocketClientFlow(WebSocketRequest("ws://localhost:7101/api/v1/subscribe"))
//
//    // the materialized value is a tuple with
//    // upgradeResponse is a Future[WebSocketUpgradeResponse] that
//    // completes or fails when the connection succeeds or fails
//    // and closed is a Future[Done] with the stream completion from the incoming sink
//    val (upgradeResponse, closed) =
//      outgoing
//        .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
//        .toMat(incoming)(Keep.both) // also keep the Future[Done]
//        .run()
//
//    // just like a regular http request we can access response status which is available via upgrade.response.status
//    // status code 101 (Switching Protocols) indicates that server support WebSockets
//    val connected = upgradeResponse.flatMap { upgrade =>
//      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
//        Future.successful(Done)
//      } else {
//        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
//      }
//    }
//
//    // in a real application you would not side effect here
//    connected.onComplete(println)
//    closed.foreach(_ => println("closed"))
//  }
}
