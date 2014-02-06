package org.kiji.scoring.server

import java.io.File
import java.io.FileOutputStream
import java.util.jar.JarOutputStream
import java.net.URL

import org.apache.commons.io.IOUtils
import org.junit.Assert
import org.junit.Test

import org.kiji.schema.KijiClientTest

class TestGetModelServlet extends KijiClientTest {
  @Test
  def testGetModelServlet() {
    val tempDir = TestUtils.setupServerEnvironment(getKiji.getURI)
    val jarFile = File.createTempFile("temp_artifact", ".jar")
    val jarOS = new JarOutputStream(new FileOutputStream(jarFile))
    TestUtils.addToJar("org/kiji/scoring/server/DummyScoreFunction.class", jarOS)
    jarOS.close()

    TestUtils.deploySampleLifecycle(getKiji, jarFile.getAbsolutePath, "0.0.1")

    val server = ScoringServer(tempDir, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
    server.start()
    try {
      val connector = server.server.getConnectors()(0)
      TestUtils.scan(server)

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
}
