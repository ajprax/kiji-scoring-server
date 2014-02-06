package org.kiji.scoring.server

import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPut

import org.junit.Assert
import org.junit.Test

import org.kiji.schema.KijiClientTest

class TestPingServlet extends KijiClientTest {

  @Test
  def testPingServlet() {
    val tempHome = TestUtils.setupServerEnvironment(getKiji.getURI)
    val server = ScoringServer(tempHome, ServerConfiguration(8080, getKiji.getURI.toString, 0, 2))
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
}
