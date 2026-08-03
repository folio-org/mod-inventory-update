package org.folio.inventoryupdate.unittests.fixtures;

import com.sun.net.httpserver.SimpleFileServer;
import com.sun.net.httpserver.SimpleFileServer.OutputLevel;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Path;

public class FileService {
  private static HttpServer fileServer;
  public static void main (String[] args) {
    start(8091);
  }

  public static void start(int port) {
    if (fileServer == null) {
      fileServer = SimpleFileServer.createFileServer(
          new InetSocketAddress(port),
          Path.of(System.getProperty("user.dir"),
              "src","test","resources","fixtures","samplesourcefiles","remote"),
          OutputLevel.VERBOSE);
      fileServer.start();
    }
  }
}
