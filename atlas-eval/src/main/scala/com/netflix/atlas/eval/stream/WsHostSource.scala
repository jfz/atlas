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
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.model.ws.WebSocketRequest
import akka.http.scaladsl.model.ws.WebSocketUpgradeResponse
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.eval.stream.SubscriptionManager.UriAndExpressions
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Helper for creating a websocket stream source for a given host.
  */
private[stream] object WsHostSource extends StrictLogging {

  def apply(
    context: StreamContext,
    uriAndExpressions: UriAndExpressions,
    delay: FiniteDuration = 1.second
  ): Source[ByteString, NotUsed] = {
    EvaluationFlows.repeat(uriAndExpressions, delay).flatMapConcat(singleCall(context))
  }

  private def singleCall(
    context: StreamContext
  )(uriAndExpressions: UriAndExpressions): Source[ByteString, Any] = {

    implicit val system = context.system
    implicit val mat = context.materializer
    implicit val ec = system.dispatcher

    val uri = uriAndExpressions.uri
    val expressionsStr = Json.encode(uriAndExpressions.expressions)

    //TODO how to auto recovery on disconnection
    val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] =
      Http(context.system).webSocketClientFlow(WebSocketRequest(uri))

     ////###################################################################
        Source
          .single(TextMessage(expressionsStr))
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
          .map(ByteString(_))


    ////####################################################################
//    // Flow used for logging diagnostic messages
//    val (queue, pub) = StreamOps
//      .blockingQueue[Message](context.registry, "WebSocketDataQueue", 10)
//      .toMat(Sink.asPublisher(true))(Keep.both)
//      .run()
//
//    val sink: Sink[Message, Future[Done]] =
//      Sink.foreach[Message] {
//        case message: Message =>
//          queue.offer(message)
//      }
//
//    val (upgradeResponse, closed) =
//      Source
//        .single(TextMessage(expressionsStr))
//        .concatMat(Source.maybe[TextMessage])(Keep.right)
//        .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
//        .toMat(sink)(Keep.both) // also keep the Future[Done]
//        .run()
//
//    val source = Source
//      .fromPublisher(pub)
//      .flatMapConcat {
//        case msg: TextMessage =>
//          msg.textStream.fold("")(_ + _)
//        case msg: BinaryMessage =>
//          msg.dataStream
//            .fold(ByteString.empty)(_ ++ _)
//            .map(_.decodeString(StandardCharsets.UTF_8))
//      }
//      .watchTermination() { (_, f) =>
//        f.onComplete {
//          case Success(_) =>
//            logger.info("@@@ Complete with Success")
//          case Failure(t) =>
//            logger.info("@@@ Complete with Failure", t)
//        }
//      }
//      .map(ByteString(_))
//
//    source

  }
}
