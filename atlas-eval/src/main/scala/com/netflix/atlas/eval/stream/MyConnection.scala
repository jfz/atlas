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
package com.netflix.atlas.eval.stream

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.model.ws.WebSocketRequest
import akka.http.scaladsl.model.ws.WebSocketUpgradeResponse
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

private[stream] class MyConnection(system: ActorSystem)
    extends GraphStage[FlowShape[String, Source[String, AnyRef]]]
    with StrictLogging {

  private val in = Inlet[String]("MyConnection.in")
  private val out = Outlet[Source[String, AnyRef]]("MyConnection.out")

  override val shape: FlowShape[String, Source[String, AnyRef]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {

      var uri = ""
      var webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = null


      setHandlers(in, out, this)
      //TODO 1: update expressions from external after flow is running
      //        - queue

      //TODO 2: need to explicitly close it if removed from "Cluster":
      //        - msg in should be (host, expressions, close?)
      //        - idleTimeout and let it fail and clean up with postStop

      override def onPush(): Unit = {
        val str = grab(in)
        //TODO get expr from input
        uri = "ws://127.0.0.1:7101/api/v1/subscribe"

        if(webSocketFlow != null){
          return
        }

        webSocketFlow = Http(system).webSocketClientFlow(WebSocketRequest(uri))

        val exprStr = """[{"expression":"name,jvm.gc.pause,:eq,:sum","step":10000}]"""

        val source = Source
          .single(TextMessage(exprStr))
          //Add maybe to prevent the source complete too early
          .concatMat(Source.maybe[TextMessage])(Keep.right)
          .via(webSocketFlow)
          .flatMapConcat {
            case msg: TextMessage =>
              msg.textStream.fold("")(_ + _)
            case msg: BinaryMessage =>
              msg.dataStream
                .fold(ByteString.empty)(_ ++ _)
                .map(_.decodeString(StandardCharsets.UTF_8))
          }
        push(out, source)
      }

      override def onPull(): Unit = {
        if (!isClosed(in))
          pull(in)
      }
    }
  }
}
