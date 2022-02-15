package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

import java.net.URI;
import java.time.Duration;

public class ConfigurationLoader {


  private static final String CONFIGURATION_PREFIX = "it.agilelab.bigdata.wasp.aws.auth";
  private static final String CREDENTIALS_STORAGE_KEY = "storage";
  private static final String CHECKPOINT_BUCKET_KEY = "checkpointbucket";
  private static final String DELEGATE_RENEWER_KEY = "delegate";
  private static final String RENEW_MILLIS_KEY = "renewmillis";


  private static Duration lookupRenewalTime(String bucket, Configuration configuration, Duration defaultValue) {
    String keyForBucket = CONFIGURATION_PREFIX + "." + bucket + "." + RENEW_MILLIS_KEY;
    String globalKey = CONFIGURATION_PREFIX + "." + RENEW_MILLIS_KEY;
    return Duration
        .ofMillis(configuration.getLong(keyForBucket, configuration.getLong(globalKey, defaultValue.toMillis())));
  }

  private static URI lookupCheckpointBucket(String bucket, Configuration configuration, String defaultValue) {
    String keyForBucket = CONFIGURATION_PREFIX + "." + bucket + "." + CHECKPOINT_BUCKET_KEY;
    String globalKey = CONFIGURATION_PREFIX + "." + CHECKPOINT_BUCKET_KEY;
    return new Path(configuration.getTrimmed(keyForBucket, configuration.getTrimmed(globalKey, defaultValue))).toUri();
  }

  private static Class<? extends AWSCredentialsProvider> lookupDelegateRenewer(String bucket,
                                                                               Configuration configuration,
                                                                               Class<? extends AWSCredentialsProvider> defaultValue) {
    String keyForBucket = CONFIGURATION_PREFIX + "." + bucket + "." + DELEGATE_RENEWER_KEY;
    String globalKey = CONFIGURATION_PREFIX + "." + DELEGATE_RENEWER_KEY;
    return configuration.getClass(keyForBucket, configuration.getClass(globalKey, defaultValue,
        AWSCredentialsProvider.class), AWSCredentialsProvider.class);
  }


  private static Path lookupCredentialsStoragePath(String bucket, Configuration configuration, String defaultValue) {
    String keyForBucket = CONFIGURATION_PREFIX + "." + bucket + "." + CREDENTIALS_STORAGE_KEY;
    String globalKey = CONFIGURATION_PREFIX + "." + CREDENTIALS_STORAGE_KEY;
    return new Path(configuration.getTrimmed(keyForBucket, configuration.getTrimmed(globalKey, defaultValue)));
  }


  public static ProviderConfiguration lookupConfig(URI bucket, Configuration configuration) {
    return new ProviderConfiguration(bucket, lookupCredentialsStoragePath(bucket.getHost(), configuration, "/tmp"),
        lookupRenewalTime(bucket.getHost(), configuration, Duration.ofMinutes(30)),
        lookupDelegateRenewer(bucket.getHost(), configuration, SimpleAWSCredentialsProvider.class),
        configuration, lookupCheckpointBucket(bucket.getHost(), configuration, "/tmp")
    );
  }
}
