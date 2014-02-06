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

import java.io.BufferedInputStream
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.net.URL
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import com.google.common.io.Files
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.SerializationUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.DefaultHttpClient
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

class TestScoringServer extends KijiClientTest {

  var mTempHome: File = null
  val emailAddress = "name@company.com"
  val tableLayout = KijiTableLayouts.getLayout("org/kiji/scoring/server/sample/user_table.json")

  private def scan(server: ScoringServer) {
    val port = server.server.getConnectors()(0).getLocalPort
    val client = new DefaultHttpClient()
    val get = new HttpGet("http://localhost:%s/admin/scanner".format(port))
    val response = client.execute(get)
    try {
      Assert.assertEquals(200, response.getStatusLine.getStatusCode)
    } finally {
      get.releaseConnection()
    }
    server.overlayedProvider.scan()
    Thread.sleep(2000)
  }

  @Before
  def setup() {
    new InstanceBuilder(getKiji)
        .withTable(tableLayout)
            .withRow(12345: java.lang.Long)
                .withFamily("info")
                    .withQualifier("email")
                        .withValue(1, emailAddress)
    .build

    val tempModelRepoDir = Files.createTempDir()
    tempModelRepoDir.deleteOnExit()
    KijiModelRepository.install(getKiji, tempModelRepoDir.toURI)

    mTempHome = TestUtils.setupServerEnvironment(getKiji.getURI)
  }

  @After
  def tearDown() {
    mTempHome.delete()
  }

  @Test
  def testShouldDeployAndRunSingleLifecycle() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      scan(server)
      val connector = server.server.getConnectors()(0)
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
      assert(Integer.parseInt(response.getValue) == emailAddress.length())
    } finally {
      server.stop()
    }
  }

  @Test
  def testShouldHotUndeployModelLifecycle() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      scan(server)
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))

      assert(Integer.parseInt(response.getValue.toString) == emailAddress.length())

      val modelRepoTable = getKiji.openTable("model_repo")
      try {
        val writer = modelRepoTable.openTableWriter()
        try {
          writer.put(modelRepoTable.getEntityId(TestUtils.artifactName,
            "0.0.1"), "model", "production_ready", false)
        } finally {
          writer.close()
        }
        modelRepoTable.release()
        scan(server)
        try {
          TestUtils.scoringServerResponse(connector.getLocalPort,
            "org/kiji/test/sample_model/0.0.1/?eid=[12345]&request=" +
                Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
          Assert.fail("Scoring server should have thrown a 404 but didn't")
        } catch {
          case ex: FileNotFoundException => ()
        }
      }
    } finally {
      server.stop()
    }
  }

  @Test
  def testCanPassFreshParameters() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      scan(server)
      // "%3D%3D%3D is a url encoding of '==='.
      val response = TestUtils.scoringServerResponse(connector.getLocalPort,
        "org/kiji/test/sample_model/0.0.1/?eid=[12345]&fresh.jennyanydots=%3D%3D%3D&request=" +
            Base64.encodeBase64String(SerializationUtils.serialize(KijiDataRequest.empty())))
      assert(Integer.parseInt(response.getValue.toString) == "===".length())
    } finally {
      server.stop()
    }
  }

  @Test
  def testListModelsServlet() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      scan(server)

      val url = new URL("http://localhost:%s/admin/list".format(connector.getLocalPort))
      val response = IOUtils.toString(url.openStream(), "UTF-8")
      Assert.assertEquals(
          """{
            |  {"org.kiji.test.sample_model-0.0.1":"models/org/kiji/test/sample_model/0.0.1"}
            |}""".stripMargin,
          response
      )
    } finally {
      server.stop()
    }
  }

  @Test
  def testGetModelServlet() {
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      scan(server)

      val url = new URL("http://localhost:%s/admin/get?model=%s".format(
        connector.getLocalPort, "org.kiji.test.sample_model-0.0.1"))
      val response = IOUtils.toString(url.openStream(), "UTF-8")
      Assert.assertEquals(
          """{"org.kiji.test.sample_model-0.0.1":"models/org/kiji/test/sample_model/0.0.1"}""",
          response
      )
    } finally {
      server.stop()
    }
  }

  @Test
  def testPingServlet() {
    val server = ScoringServer(mTempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      val url = "http://localhost:%s/admin/ping".format(connector.getLocalPort)

      {
        // The server should return pings normally.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to Hidden.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Hidden")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be hidden.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(404, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to Unhealthy.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Unhealthy")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be unhealthy.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(500, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it back to Healthy.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=Healthy")
        val response = client.execute(put)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          put.releaseConnection()
        }
      }
      {
        // Server should be Healthy.
        val client = new DefaultHttpClient()
        val get = new HttpGet(url)
        val response = client.execute(get)
        try {
          Assert.assertEquals(200, response.getStatusLine.getStatusCode)
        } finally {
          get.releaseConnection()
        }
      }
      {
        // Set it to an unknown status.
        val client = new DefaultHttpClient()
        val put = new HttpPut(url + "?status=invalid")
        val response = client.execute(put)
        try {
          Assert.assertEquals(400, response.getStatusLine.getStatusCode)
          Assert.assertEquals(
              "unknown server status: invalid", response.getStatusLine.getReasonPhrase)
        } finally {
          put.releaseConnection()
        }
      }
    } finally {
      server.stop()
    }
  }

  /**
   * Adds the given classFile to the target JAR output stream. The classFile is assumed to
   * be a resource on the classpath.
   * @param classFile is the class file name to add to the jar file.
   * @param target is the outputstream representing the jar file where the class gets written.
   */
  def addToJar(classFile: String, target: JarOutputStream) {
    val inStream = getClass.getClassLoader.getResourceAsStream(classFile)
    val entry = new JarEntry(classFile)
    target.putNextEntry(entry)
    val in = new BufferedInputStream(inStream)

    val buffer = new Array[Byte](1024)
    var count = in.read(buffer)
    while (count >= 0) {
      target.write(buffer, 0, count)
      count = in.read(buffer)
    }
    target.closeEntry()
    in.close()
  }
}
