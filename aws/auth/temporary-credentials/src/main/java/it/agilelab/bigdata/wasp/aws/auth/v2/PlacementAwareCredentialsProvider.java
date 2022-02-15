package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class PlacementAwareCredentialsProvider implements AWSCredentialsProvider {

  private final Logger logger = LoggerFactory.getLogger(PlacementAwareCredentialsProvider.class);

  AWSCredentialsProvider downstream;

  public PlacementAwareCredentialsProvider(URI fsUri, Configuration conf)
      throws IOException {

    String property = System.getProperty("sun.java.command");
    URI checkpointBucketUri = ConfigurationLoader.lookupConfig(fsUri, conf).getCheckpointBucket();
    logger.info("FsUri: [{}]", fsUri);
    logger.info("CheckpointBucketUri: [{}]", checkpointBucketUri);
    logger.info("FsUri host equals checkpointBucketUri: [{}]", fsUri.getHost().equals(checkpointBucketUri.getHost()));
    if (property.contains("CoarseGrainedExecutorBackend") || !fsUri.getHost().equals(checkpointBucketUri.getHost())) {
      logger.info("Instantiating {}", ExecutorSideAssumeRoleCredentialsProvider.class);
      downstream = new ExecutorSideAssumeRoleCredentialsProvider(fsUri, conf);
    } else {
      logger.info("Instantiating {}", DriverSideAssumeRoleCredentialsProvider.class);
      downstream = new DriverSideAssumeRoleCredentialsProvider(fsUri, conf);
    }


  }

  @Override
  public AWSCredentials getCredentials() {
    return downstream.getCredentials();
  }

  @Override
  public void refresh() {
    downstream.refresh();
  }
}
