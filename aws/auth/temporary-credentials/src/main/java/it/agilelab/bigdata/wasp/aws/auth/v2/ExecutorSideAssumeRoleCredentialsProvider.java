package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ExecutorSideAssumeRoleCredentialsProvider implements AWSCredentialsProvider {

  ProviderConfiguration configuration;

  public ExecutorSideAssumeRoleCredentialsProvider(URI fsUri, Configuration conf)
      throws IOException {

    configuration = ConfigurationLoader.lookupConfig(fsUri, conf);
  }

  @Override
  public AWSCredentials getCredentials() {
    Path path = new Path(configuration.getStoragePath(), configuration.getBucket().getHost());
    try {
      FileSystem fs = path.getFileSystem(configuration.getConfiguration());
      return CredentialsSerde.read(fs, path);
    } catch (IOException e) {
      throw new RuntimeException("Cannot read credentials", e);
    }
  }

  @Override
  public void refresh() {

  }
}
