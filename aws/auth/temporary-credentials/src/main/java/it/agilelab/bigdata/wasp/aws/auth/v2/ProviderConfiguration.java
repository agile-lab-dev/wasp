package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.time.Duration;

public class ProviderConfiguration {

  private URI bucket;
  private Path storagePath;
  private Duration renewalTime;
  private Class<? extends AWSCredentialsProvider> delegateProvider;
  private Configuration configuration;
  private URI checkpointBucket;

  public ProviderConfiguration(URI bucket, Path storagePath, Duration renewalTime,
                               Class<? extends AWSCredentialsProvider> delegateProvider, Configuration configuration, URI checkpointBucket) {
    this.bucket = bucket;
    this.storagePath = storagePath;
    this.renewalTime = renewalTime;
    this.delegateProvider = delegateProvider;
    this.configuration = configuration;
    this.checkpointBucket = checkpointBucket;
  }

  public URI getBucket() {
    return bucket;
  }

  public Path getStoragePath() {
    return storagePath;
  }

  public Duration getRenewalTime() {
    return renewalTime;
  }

  public Class<? extends AWSCredentialsProvider> getDelegateProvider() {
    return delegateProvider;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  AWSCredentialsProvider instantiate() throws Exception {
    return delegateProvider.getConstructor(URI.class, Configuration.class).newInstance(bucket, configuration);
  }

  public URI getCheckpointBucket() {
    return checkpointBucket;
  }
}
