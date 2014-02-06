/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.scoring.server

import java.io.File
import java.io.PrintWriter
import java.net.URL
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Files

import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.modelrepo.tools.DeployModelRepoTool
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

object TestUtils {
  val artifactName = "org.kiji.test.sample_model"

  val modelContainerTemplate =
    """{
      |  "model_name": "name",
      |  "model_version": "1.0.0",
      |  "score_function_class": "org.kiji.scoring.server.DummyScoreFunction",
      |  "parameters": {},
      |  "table_uri": "%s",
      |  "column_name": "%s",
      |  "record_version": "model_container-0.1.0"
      |}
    """.stripMargin

  /**
   * Deploys the specified artifact file to the model repository associated
   * with the specified kiji instance. <b>Note:</b> that the name for this lifecycle
   * is fixed at "org.kiji.test.sample_lifecycle". The KijiModelContainer JSON template is available
   * with the name modelContainerTemplate in this file; it is populated with tableURI build from the
   * KijiURI of the given Kiji instance.
   *
   * @param kiji instance in which the test will operate.
   * @param artifactFile path to the model JAR to deploy.
   * @param version of the model.
   */
  def deploySampleLifecycle(kiji: Kiji, artifactFile: String, version: String) {
    val tempContainerFile = new File(Files.createTempDir(), "model_container.json")
    val modelContainerJson = modelContainerTemplate.format(
        KijiURI.newBuilder(kiji.getURI).withTableName("users").build(), "info:out")
    doAndClose(new PrintWriter(tempContainerFile, "UTF-8")) {
      pw => pw.print(modelContainerJson)
    }

    val deployTool = new DeployModelRepoTool
    // Deploy some bogus artifact. We don't care that it's not executable code yet.
    val qualifiedName = "%s-%s".format(artifactName, version)
    val args = List(
      qualifiedName,
      artifactFile,
      "--kiji=" + kiji.getURI().toString(),
      "--model-container=" + tempContainerFile.getAbsolutePath,
      "--production-ready=true",
      "--message=Uploading Artifact")

    deployTool.toolMain(args.asJava)
  }

  /**
   * Sets up the scoring server environment by creating a configuration file based on the
   * given KijiURI. Returns a handle to the temporary directory created.
   *
   * @param repo_uri is the URI of the Kiji instance.
   *
   * @return a handle to the temporary directory created.
   */
  def setupServerEnvironment(repo_uri: KijiURI): File = {
    val tempModelDir = Files.createTempDir()
    // Create the configuration folder
    val confFolder = new File(tempModelDir, "conf")
    confFolder.mkdir()
    // Create the models folder
    new File(tempModelDir, "models/webapps").mkdirs()
    new File(tempModelDir, "models/instances").mkdirs()
    new File(tempModelDir, "models/templates").mkdirs()

    tempModelDir.deleteOnExit()

    val configMap = Map(
        "port" -> 0,
        "repo_uri" -> repo_uri.toString(),
        "repo_scan_interval" -> 2,
        "num_acceptors" -> 2)

    val mapper = new ObjectMapper()
    mapper.writeValue(new File(confFolder, "configuration.json"), configMap.asJava)

    tempModelDir
  }

  /**
   * Returns the KijiScoringServerCell result from scoring a model.
   *
   * @param httpPort running the scoring server.
   * @param endPoint with any parameters to request.
   *
   * @return the parsed JSON response as a KijiScoringServerCell.
   */
  def scoringServerResponse(httpPort: Int, endPoint: String): KijiScoringServerCell = {
    val formattedUrl = "http://localhost:%s/models/%s".format(httpPort, endPoint)
    val endpointUrl = new URL(formattedUrl)
    val jsonMapper = new ObjectMapper
    jsonMapper.readValue(endpointUrl, classOf[KijiScoringServerCell])
  }
}
