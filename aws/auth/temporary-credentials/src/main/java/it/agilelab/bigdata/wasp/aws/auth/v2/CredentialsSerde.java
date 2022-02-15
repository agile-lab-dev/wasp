package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CredentialsSerde {

  private static Logger log = LoggerFactory.getLogger(CredentialsSerde.class);

  private static void writeSessionCredentials(AWSSessionCredentials credentials, OutputStreamWriter out)
      throws IOException {
    out.write("SESSION");
    out.write("\n");
    out.write(credentials.getAWSAccessKeyId());
    out.write("\n");
    out.write(credentials.getAWSSecretKey());
    out.write("\n");
    out.write(credentials.getSessionToken());
    out.flush();
  }


  private static void writeCredentials(AWSCredentials credentials, OutputStreamWriter out) throws IOException {
    out.write("PLAIN");
    out.write("\n");
    out.write(credentials.getAWSAccessKeyId());
    out.write("\n");
    out.write(credentials.getAWSSecretKey());
    out.write("\n");
    out.flush();
  }

  public static void write(Configuration conf, Path storageLocation, AWSCredentials credentials, String filename) {

    try {

      FileSystem fileSystem = storageLocation.getFileSystem(conf);

      fileSystem.mkdirs(storageLocation);

      Path tokenPath = new Path(storageLocation, filename);

      try (
          OutputStreamWriter out = new OutputStreamWriter(fileSystem.create(tokenPath, true), StandardCharsets.UTF_8)) {

        if (credentials instanceof AWSSessionCredentials) {
          writeSessionCredentials((AWSSessionCredentials) credentials, out);
        } else {
          writeCredentials(credentials, out);
        }

      }
    } catch (IOException ex) {
      throw new RuntimeException("Cannot write credentials", ex);
    }

  }


  public static AWSCredentials read(FileSystem fileSystem, Path storageLocation) throws IOException {

    log.info("Getting executor credentials");

    String lastFile = "";

    RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(storageLocation, false);

    while (locatedFileStatusRemoteIterator.hasNext()) {
      String filename = locatedFileStatusRemoteIterator.next().getPath().getName();

      if (filename.compareTo(lastFile) > 0) {
        lastFile = filename;
      }
    }

    log.info("SELECTED " + lastFile);
    Path tokenPath = new Path(storageLocation, lastFile);

    try (
        BufferedReader in = new BufferedReader(
            new InputStreamReader(fileSystem.open(tokenPath), StandardCharsets.UTF_8))) {

      String type = in.readLine();

      if (type.equals("SESSION")) {

        String keyId = in.readLine();
        String secret = in.readLine();
        String session = in.readLine();

        return new AWSSessionCredentials() {

          @Override
          public String getAWSAccessKeyId() {
            return keyId;
          }

          @Override
          public String getAWSSecretKey() {
            return secret;
          }

          @Override
          public String getSessionToken() {
            return session;
          }
        };

      } else {

        String keyId = in.readLine();
        String secret = in.readLine();

        return new AWSCredentials() {
          @Override
          public String getAWSAccessKeyId() {
            return keyId;
          }

          @Override
          public String getAWSSecretKey() {
            return secret;
          }
        };
      }

    }
  }

  public static void cleanupOldCredentials(Configuration conf, Path storageLocation) throws IOException {
    FileSystem fileSystem = storageLocation.getFileSystem(conf);

    if (fileSystem.exists(storageLocation)) {
      RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(storageLocation, false);

      List<String> files = new ArrayList<>();

      while (locatedFileStatusRemoteIterator.hasNext()) {
        String filename = locatedFileStatusRemoteIterator.next().getPath().getName();

        files.add(filename);
      }

      List<String> filesToRemove =
              files.stream().sorted().limit(Math.max(0, files.size() - 5))
                      .collect(Collectors.toList());

      for (String fileToRemove : filesToRemove) {
        fileSystem.delete(new Path(storageLocation, fileToRemove), false);
      }
    }
  }

}
