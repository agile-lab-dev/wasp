package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DriverSideAssumeRoleCredentialsProvider implements AWSCredentialsProvider {

  Logger logger = LoggerFactory.getLogger(DriverSideAssumeRoleCredentialsProvider.class);

  private static ConcurrentHashMap<String, CredentialRenewer> renewers = new ConcurrentHashMap<>();

  private CredentialRenewer renewer;


  public DriverSideAssumeRoleCredentialsProvider(URI fsUri, Configuration conf)
      throws IOException {

    String bucketName = fsUri.getHost();

    renewer = renewers.computeIfAbsent(bucketName, k -> {
      logger.info("Instantiating renewer for {}", bucketName);
      return new DefaultCredentialRenewer(ConfigurationLoader.lookupConfig(fsUri, conf));
    });
  }


  @Override
  public AWSCredentials getCredentials() {
    return renewer.getCredentials();
  }

  @Override
  public void refresh() {
    renewer.refresh();
  }
}


interface CredentialRenewer extends AWSCredentialsProvider {

}

class DefaultCredentialRenewer implements CredentialRenewer {

  private ProviderConfiguration conf;

  Logger log = LoggerFactory.getLogger(DefaultCredentialRenewer.class);

  AWSCredentialsProvider downstream;

  private static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private AWSCredentials currentCredentials = null;

  DefaultCredentialRenewer(ProviderConfiguration conf) {
    this.conf = conf;

    try {
      downstream = conf.instantiate();
      log.info("Downstream renewer is of type {}", downstream.getClass());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.renew();
    scheduler.scheduleAtFixedRate(this::renew, conf.getRenewalTime().toMillis(), conf.getRenewalTime().toMillis(),
        TimeUnit.MILLISECONDS);
  }

  @Override
  public AWSCredentials getCredentials() {
    synchronized (this) {
      return currentCredentials;
    }
  }

  @Override
  public void refresh() {

  }

  private void renew() {
    synchronized (this) {
      try {

        log.info("Renewing credentials");
        downstream.refresh();
        currentCredentials = downstream.getCredentials();
        log.info("Credentials renewed");

        Path bucketTokensPath = new Path(conf.getStoragePath(), conf.getBucket().getHost());

        log.info("Will write file to {}", bucketTokensPath);

        String maxLongAsString = Long.toString(Long.MAX_VALUE);

        String now = Long.toString(Instant.now().toEpochMilli());

        String paddedDeadline = Strings.padStart(now, maxLongAsString.length(), '0');

        log.info("Writing credentials");
        CredentialsSerde.write(conf.getConfiguration(), bucketTokensPath, currentCredentials, paddedDeadline);
        log.info("Wrote credentials");

        CredentialsSerde.cleanupOldCredentials(conf.getConfiguration(), bucketTokensPath);
      } catch (Exception e) {
        log.error("Failed to renew credentials ", e);
      }
    }
  }


}