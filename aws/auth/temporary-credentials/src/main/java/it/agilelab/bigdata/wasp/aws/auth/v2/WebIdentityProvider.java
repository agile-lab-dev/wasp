package it.agilelab.bigdata.wasp.aws.auth.v2;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.WebIdentityFederationSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.model.ExpiredTokenException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;

public class WebIdentityProvider implements AWSCredentialsProvider {

  private final Path tokenFile;
  private final String role;

  WebIdentityFederationSessionCredentialsProvider provider;

  private static String TOKEN_FILE_KEY = "fs.s3a.assumed.role.web.tokenfile";
  private static String TOKEN_FILE_DEFAULT = "/var/run/secrets/eks.amazonaws.com/serviceaccount/token";

  private static String ROLE_ARN = "fs.s3a.assumed.role.web.role.arn";

  private final Object lock = new Object();

  public WebIdentityProvider(URI uri, Configuration configuration) throws IOException {
    tokenFile = Paths.get(configuration.get(TOKEN_FILE_KEY, TOKEN_FILE_DEFAULT));
    role = configuration.get(ROLE_ARN);
    provider = instantiate(tokenFile, role);
  }


  public WebIdentityFederationSessionCredentialsProvider instantiate(Path tokenFile, String role) throws IOException {
    if (!Files.exists(tokenFile)) {
      throw new IOException("Token file [" + tokenFile + "] not found");
    }

    String token = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8);

    return new WebIdentityFederationSessionCredentialsProvider(token, null, role);
  }

  @Override
  public AWSCredentials getCredentials() {
    synchronized (lock) {
      try {
        return provider.getCredentials();
      } catch (ExpiredTokenException ex) {
        try {
          provider = instantiate(tokenFile, role);
          return provider.getCredentials();
        } catch (IOException e) {
          throw new RuntimeException("Could not instantiate provider");
        }
      }
    }
  }

  @Override
  public void refresh() {
    synchronized (lock) {
      provider.refresh();
    }
  }
}
