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

import akka.NotUsed
import akka.http.scaladsl.model.Uri
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import com.netflix.atlas.eval.stream.EurekaSource.Groups
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.SubscriptionManager.Expression
import com.netflix.atlas.eval.stream.SubscriptionManager.SubscribePayload
import com.netflix.atlas.eval.stream.SubscriptionManager.UriAndExpressions
import com.netflix.atlas.json.Json
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._

/**
  * Manages a set of connections to all instances for a set of Eureka groups and collects
  * the data from the `/lwc/api/v1/stream/$id` endpoint.
  */
private[stream] class ConnectionManager(context: StreamContext)
    extends GraphStage[FlowShape[SourcesAndGroups, InstanceSources]]
    with StrictLogging {

  private val in = Inlet[SourcesAndGroups]("ConnectionManager.in")
  private val out = Outlet[InstanceSources]("ConnectionManager.out")

  override val shape: FlowShape[SourcesAndGroups, InstanceSources] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) with InHandler with OutHandler {
      private val instanceMap = scala.collection.mutable.AnyRefMap.empty[String, InstanceSourceRef]

      private def mkString(m: Map[String, _]): String = {
        m.keySet.toList.sorted.mkString(",")
      }

      private def toExpressions(ds: List[DataSource]): List[Expression] = {
        val exprs = ds.flatMap { d =>
          context.interpreter.eval(Uri(d.getUri)).map { expr =>
            Expression(expr.toString, d.getStep.toMillis)
          }
        }
        exprs
      }

      override def onPush(): Unit = {
        val sourcesAndGroups = grab(in)
        val dataSourceList = sourcesAndGroups._1.getSources.asScala.toList
        val instances = sourcesAndGroups._2.groups.flatMap(_.instances)

        val currentIds = instanceMap.keySet
        val foundInstances = instances.map(i => i.instanceId -> i).toMap

        val added = foundInstances -- currentIds
        if (added.nonEmpty) {
          logger.debug(s"instances added: ${mkString(added)}")
        }

        //val path = "/lwc/api/v1/stream/" + context.id
        val path = "/api/v1/subscribe"
        val sources = added.values.map { instance =>
          val uri = instance.substitute("http://{local-ipv4}:{port}") + path
          val wsUri = uri.replace("http:", "ws:") //TODO update config to avoid this?
          //val ref = EvaluationFlows.stoppableSource(HostSource(uri, context.httpClient("stream")))
          val ref = EvaluationFlows.stoppableSource(
            WsHostSource(context, UriAndExpressions(wsUri, toExpressions(dataSourceList)))
          )
          instanceMap += instance.instanceId -> ref
          ref.source
        }
        push(out, sources.toList)

        val removed = instanceMap.toMap -- foundInstances.keySet
        if (removed.nonEmpty) {
          logger.debug(s"instances removed: ${mkString(removed)}")
        }
        removed.foreach {
          case (id, ref) =>
            instanceMap -= id
            ref.stop()
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      setHandlers(in, out, this)
    }
  }
}
