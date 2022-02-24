import com.typesafe.sbt.packager.Keys.scriptClasspath
import sbt.Keys.{excludeDependencies, libraryDependencies, transitiveClassifiers}
import sbt._

class CDP717Dependencies(versions: CDP717Versions) extends Dependencies {

  lazy val spark_sql_kafka     = "it.agilelab"     %% "wasp-spark-sql-kafka"     % "0.1.0-2.4.1-2.4.7-7.1.7.0-551"
  lazy val spark_sql_kafka_old = "it.agilelab"     %% "wasp-spark-sql-kafka-old" % "0.1.0-2.4.1-2.4.7-7.1.7.0-551"
  lazy val delta               = "it.agilelab"     %% "wasp-delta-lake"          % "0.6.1-2.4.7.7.1.7.0-551"
  lazy val solrj               = "org.apache.solr" % "solr-solrj"                % versions.solr
  lazy val sparkSolr = versions.scala.take(4) match {
    case "2.11" => ("it.agilelab.bigdata.spark" % "spark-solr"  % versions.sparkSolr)
    case "2.12" => ("it.agilelab.bigdata.spark" %% "spark-solr" % versions.sparkSolr)
  }
  lazy val shapeless           = "com.chuusai"      %% "shapeless"                % "2.3.3"
  lazy val javaxMail = "javax.mail" % "mail" % "1.4"
  lazy val globalExclusions: Seq[ExclusionRule] = Seq(
    ExclusionRule("org.apache.logging.log4j", "log4j-slf4j-impl"),
    ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala_2.12"),
    ExclusionRule("org.scala-lang.modules", "scala-java8-compat_2.12"),
    ExclusionRule("net.jpountz.lz4", "lz4"),
    ExclusionRule("org.apache.velocity", "velocity-engine-core")
  )
  lazy val parcelDependencies = Seq(
    "aopalliance"                      % "aopalliance"                                 % "1.0",
    "cglib"                            % "cglib"                                       % "2.2.2",
    "com.aliyun"                       % "aliyun-java-sdk-core"                        % "3.4.0",
    "com.aliyun"                       % "aliyun-java-sdk-ecs"                         % "4.2.0",
    "com.aliyun"                       % "aliyun-java-sdk-ram"                         % "3.0.0",
    "com.aliyun"                       % "aliyun-java-sdk-sts"                         % "3.0.0",
    "com.aliyun.oss"                   % "aliyun-sdk-oss"                              % "3.4.1",
    "com.amazonaws"                    % "aws-java-sdk-bundle"                         % "1.11.901",
    "com.carrotsearch"                 % "hppc"                                        % "0.7.2",
    "com.cedarsoftware"                % "java-util"                                   % "1.9.0",
    "com.cedarsoftware"                % "json-io"                                     % "2.5.1",
    "com.chuusai"                      %% "shapeless"                                  % "2.3.2",
    "com.clearspring.analytics"        % "stream"                                      % "2.7.0",
    "com.cronutils"                    % "cron-utils"                                  % "9.1.3",
    "com.esotericsoftware"             % "kryo-shaded"                                 % "4.0.2",
    "com.esotericsoftware"             % "minlog"                                      % "1.3.0",
    "com.esri.geometry"                % "esri-geometry-api"                           % "2.2.0",
    "com.fasterxml.jackson.core"       % "jackson-annotations"                         % "2.10.5",
    "com.fasterxml.jackson.core"       % "jackson-core"                                % "2.10.5",
    "com.fasterxml.jackson.core"       % "jackson-databind"                            % "2.10.5.1",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"                     % "2.10.5",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile"                    % "2.10.5",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml"                     % "2.10.5",
    "com.fasterxml.jackson.jaxrs"      % "jackson-jaxrs-base"                          % "2.10.5",
    "com.fasterxml.jackson.jaxrs"      % "jackson-jaxrs-json-provider"                 % "2.10.5",
    "com.fasterxml.jackson.module"     % "jackson-module-jaxb-annotations"             % "2.10.5",
    "com.fasterxml.jackson.module"     % "jackson-module-paranamer"                    % "2.10.5",
    "com.fasterxml.jackson.module"     % "jackson-module-scala_2.11"                   % "2.10.5",
    "com.fasterxml.woodstox"           % "woodstox-core"                               % "5.0.3",
    "com.flipkart.zjsonpatch"          % "zjsonpatch"                                  % "0.3.0",
    "com.github.fommil.netlib"         % "core"                                        % "1.1.2",
    "com.github.joshelser"             % "dropwizard-metrics-hadoop-metrics2-reporter" % "0.1.2",
    "com.github.luben"                 % "zstd-jni"                                    % "1.4.3-1",
    "com.github.mifmif"                % "generex"                                     % "1.0.2",
    "com.github.stephenc.jcip"         % "jcip-annotations"                            % "1.0-1",
    "com.google.cloud.bigdataoss"      % "gcs-connector"                               % "2.1.2.7.1.7.0-551" classifier ("shaded"),
    "com.google.code.findbugs"         % "jsr305"                                      % "3.0.0",
    "com.google.code.tempus-fugit"     % "tempus-fugit"                                % "1.1",
    "com.google.errorprone"            % "error_prone_annotations"                     % "2.3.2",
    "com.google.flogger"               % "flogger"                                     % "0.5",
    "com.google.flogger"               % "flogger-slf4j-backend"                       % "0.5",
    "com.google.flogger"               % "flogger-system-backend"                      % "0.5",
    "com.google.flogger"               % "google-extensions"                           % "0.5",
    "com.google.guava"                 % "failureaccess"                               % "1.0.1",
    "com.google.guava"                 % "guava"                                       % "28.1-jre",
    "com.google.guava"                 % "listenablefuture"                            % "9999.0-empty-to-avoid-conflict-with-guava",
    "com.google.inject"                % "guice"                                       % "4.0",
    "com.google.inject.extensions"     % "guice-servlet"                               % "4.0",
    "com.google.j2objc"                % "j2objc-annotations"                          % "1.3",
    "com.google.protobuf"              % "protobuf-java"                               % "2.5.0",
    "com.google.re2j"                  % "re2j"                                        % "1.2",
    "com.googlecode.json-simple"       % "json-simple"                                 % "1.1.1",
    "com.jayway.jsonpath"              % "json-path"                                   % "2.4.0",
    "com.jcraft"                       % "jsch"                                        % "0.1.54",
    "com.jolbox"                       % "bonecp"                                      % "0.8.0.RELEASE",
    "com.lmax"                         % "disruptor"                                   % "3.3.7",
    "com.microsoft.azure"              % "azure-data-lake-store-sdk"                   % "2.3.6",
    "com.microsoft.azure"              % "azure-keyvault-core"                         % "1.0.0",
    "com.microsoft.azure"              % "azure-storage"                               % "7.0.0",
    "com.microsoft.sqlserver"          % "mssql-jdbc"                                  % "6.2.1.jre7",
    "com.nimbusds"                     % "nimbus-jose-jwt"                             % "7.9",
    "com.ning"                         % "compress-lzf"                                % "1.0.3",
    "com.squareup.okhttp3"             % "logging-interceptor"                         % "3.12.6",
    "com.squareup.okhttp3"             % "okhttp"                                      % "3.12.6",
    "com.squareup.okio"                % "okio"                                        % "1.15.0",
    "com.stumbleupon"                  % "async"                                       % "1.4.1",
    "com.sun.istack"                   % "istack-commons-runtime"                      % "3.0.8",
    "com.sun.jersey"                   % "jersey-core"                                 % "1.19",
    "com.sun.jersey"                   % "jersey-json"                                 % "1.19",
    "com.sun.jersey"                   % "jersey-servlet"                              % "1.19",
    "com.sun.jersey.contribs"          % "jersey-guice"                                % "1.19",
    "com.sun.jersey.contribs"          % "jersey-multipart"                            % "1.19",
    "com.sun.xml.bind"                 % "jaxb-impl"                                   % "2.2.3-1",
    "com.tdunning"                     % "json"                                        % "1.8",
    "com.thoughtworks.paranamer"       % "paranamer"                                   % "2.8",
    "com.twitter"                      % "chill-java"                                  % "0.9.3",
    "com.twitter"                      %% "chill"                                      % "0.9.3",
    "com.univocity"                    % "univocity-parsers"                           % "2.8.3",
    "com.vlkan"                        % "flatbuffers"                                 % "1.2.0-3f79e055",
    "com.yahoo.datasketches"           % "memory"                                      % "0.9.0",
    "com.yahoo.datasketches"           % "sketches-core"                               % "0.9.0",
    "com.zaxxer"                       % "HikariCP"                                    % "2.6.1",
    "com.zaxxer"                       % "HikariCP-java7"                              % "2.4.12",
    "commons-beanutils"                % "commons-beanutils"                           % "1.9.4",
    "commons-cli"                      % "commons-cli"                                 % "1.2",
    "commons-codec"                    % "commons-codec"                               % "1.14",
    "commons-collections"              % "commons-collections"                         % "3.2.2",
    "commons-configuration"            % "commons-configuration"                       % "1.10",
    "commons-daemon"                   % "commons-daemon"                              % "1.0.13",
    "commons-dbcp"                     % "commons-dbcp"                                % "1.4",
    "commons-digester"                 % "commons-digester"                            % "1.8.1",
    "commons-httpclient"               % "commons-httpclient"                          % "3.1",
    "commons-io"                       % "commons-io"                                  % "2.6",
    "commons-lang"                     % "commons-lang"                                % "2.6",
    "commons-logging"                  % "commons-logging"                             % "1.1.3",
    "commons-net"                      % "commons-net"                                 % "3.6",
    "commons-pool"                     % "commons-pool"                                % "1.5.4",
    "commons-validator"                % "commons-validator"                           % "1.6",
    "de.ruedigermoeller"               % "fst"                                         % "2.50",
    "de.thetaphi"                      % "forbiddenapis"                               % "2.7",
    "dk.brics.automaton"               % "automaton"                                   % "1.11-8",
    "dnsjava"                          % "dnsjava"                                     % "2.1.7",
    "io.airlift"                       % "aircompressor"                               % "0.10",
    "io.dropwizard.metrics"            % "metrics-core"                                % "3.2.4",
    "io.dropwizard.metrics"            % "metrics-graphite"                            % "3.1.5",
    "io.dropwizard.metrics"            % "metrics-json"                                % "3.1.5",
    "io.dropwizard.metrics"            % "metrics-jvm"                                 % "3.1.5",
    "io.fabric8"                       % "kubernetes-client"                           % "4.6.4",
    "io.fabric8"                       % "kubernetes-model"                            % "4.6.4",
    "io.fabric8"                       % "kubernetes-model-common"                     % "4.6.4",
    "io.netty"                         % "netty"                                       % "3.10.6.Final",
    "io.netty"                         % "netty-all"                                   % "4.1.63.Final",
    "io.netty"                         % "netty-buffer"                                % "4.1.60.Final",
    "io.netty"                         % "netty-common"                                % "4.1.60.Final",
    "io.swagger"                       % "swagger-annotations"                         % "1.5.4",
    "jakarta.activation"               % "jakarta.activation-api"                      % "1.2.1",
    "jakarta.annotation"               % "jakarta.annotation-api"                      % "1.3.5",
    "jakarta.validation"               % "jakarta.validation-api"                      % "2.0.2",
    "jakarta.ws.rs"                    % "jakarta.ws.rs-api"                           % "2.1.6",
    "jakarta.xml.bind"                 % "jakarta.xml.bind-api"                        % "2.3.2",
    "com.sun.activation"               % "javax.activation"                            % "1.2.0",
    "javax.activation"                 % "javax.activation-api"                        % "1.2.0",
    "javax.annotation"                 % "javax.annotation-api"                        % "1.3.2",
    "javax.inject"                     % "javax.inject"                                % "1",
    "javax.servlet"                    % "javax.servlet-api"                           % "3.1.0",
    "javax.servlet.jsp"                % "javax.servlet.jsp-api"                       % "2.3.1",
    "javax.transaction"                % "transaction-api"                             % "1.1",
    "javax.validation"                 % "validation-api"                              % "1.1.0.Final",
    "javax.ws.rs"                      % "javax.ws.rs-api"                             % "2.0.1",
    "javax.ws.rs"                      % "jsr311-api"                                  % "1.1.1",
    "javax.xml.bind"                   % "jaxb-api"                                    % "2.2.11",
    "javolution"                       % "javolution"                                  % "5.5.1",
    "jdom"                             % "jdom"                                        % "1.1",
    "jline"                            % "jline"                                       % "2.14.6",
    "joda-time"                        % "joda-time"                                   % "2.10.6",
    "net.hydromatic"                   % "aggdesigner-algorithm"                       % "6.0",
    "net.java.dev.jna"                 % "jna"                                         % "5.2.0",
//    "net.jpountz.lz4"                  % "lz4"                                            % "1.2.0",
    "net.minidev"                     % "accessors-smart"                                % "1.2",
    "net.minidev"                     % "json-smart"                                     % "2.3",
    "net.razorvine"                   % "pyrolite"                                       % "4.13",
    "net.sf.jpam"                     % "jpam"                                           % "1.1",
    "net.sf.opencsv"                  % "opencsv"                                        % "2.3",
    "net.sf.py4j"                     % "py4j"                                           % "0.10.7",
    "net.sf.supercsv"                 % "super-csv"                                      % "2.2.0",
    "net.sourceforge.f2j"             % "arpack_combined_all"                            % "0.1",
    "net.sourceforge.jtransforms"     % "jtransforms"                                    % "2.4.0",
    "org.abego.treelayout"            % "org.abego.treelayout.core"                      % "1.0.1",
    "org.antlr"                       % "ST4"                                            % "4.0.4",
    "org.antlr"                       % "antlr-runtime"                                  % "3.5.2",
    "org.antlr"                       % "antlr4-runtime"                                 % "4.7",
    "org.apache.ant"                  % "ant"                                            % "1.10.9",
    "org.apache.ant"                  % "ant-launcher"                                   % "1.10.9",
    "org.apache.arrow"                % "arrow-format"                                   % "0.10.0",
    "org.apache.arrow"                % "arrow-memory"                                   % "0.10.0",
    "org.apache.arrow"                % "arrow-vector"                                   % "0.10.0",
    "org.apache.atlas"                % "atlas-client-common"                            % "2.1.0.7.1.7.0-551",
    "org.apache.atlas"                % "atlas-client-v2"                                % "2.1.0.7.1.7.0-551",
    "org.apache.atlas"                % "atlas-intg"                                     % "2.1.0.7.1.7.0-551",
    "org.apache.atlas"                % "atlas-plugin-classloader"                       % "2.1.0.7.1.7.0-551",
    "org.apache.atlas"                % "hive-bridge-shim"                               % "2.1.0.7.1.7.0-551",
    "org.apache.avro"                 % "avro"                                           % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-compiler"                                  % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-ipc"                                       % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-maven-plugin"                              % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-protobuf"                                  % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-service-archetype"                         % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "avro-thrift"                                    % "1.8.2.7.1.7.0-551",
    "org.apache.avro"                 % "trevni-core"                                    % "1.8.2.7.1.7.0-551",
    "org.apache.calcite"              % "calcite-core"                                   % "1.19.0.7.1.7.0-551",
    "org.apache.calcite"              % "calcite-druid"                                  % "1.19.0.7.1.7.0-551",
    "org.apache.calcite"              % "calcite-linq4j"                                 % "1.19.0.7.1.7.0-551",
    "org.apache.calcite.avatica"      % "avatica"                                        % "1.16.0.7.1.7.0-551",
    "org.apache.calcite.avatica"      % "avatica-core"                                   % "1.16.0.7.1.7.0-551",
    "org.apache.calcite.avatica"      % "avatica-metrics"                                % "1.16.0.7.1.7.0-551",
    "org.apache.commons"              % "commons-collections4"                           % "4.1",
    "org.apache.commons"              % "commons-compress"                               % "1.19",
    "org.apache.commons"              % "commons-configuration2"                         % "2.1.1",
    "org.apache.commons"              % "commons-dbcp2"                                  % "2.5.0",
    "org.apache.commons"              % "commons-lang3"                                  % "3.9",
    "org.apache.commons"              % "commons-math3"                                  % "3.6.1",
    "org.apache.commons"              % "commons-pool2"                                  % "2.6.0",
    "org.apache.curator"              % "curator-client"                                 % "4.3.0.7.1.7.0-551",
    "org.apache.curator"              % "curator-framework"                              % "4.3.0.7.1.7.0-551",
    "org.apache.curator"              % "curator-recipes"                                % "4.3.0.7.1.7.0-551",
    "org.apache.derby"                % "derby"                                          % "10.14.2.0",
    "org.apache.druid.extensions"     % "druid-bloom-filter"                             % "0.17.1.7.1.7.0-551",
    "org.apache.druid.extensions"     % "druid-hdfs-storage"                             % "0.17.1.7.1.7.0-551",
    "org.apache.druid.extensions"     % "mysql-metadata-storage"                         % "0.17.1.7.1.7.0-551",
    "org.apache.druid.extensions"     % "postgresql-metadata-storage"                    % "0.17.1.7.1.7.0-551",
    "org.apache.geronimo.specs"       % "geronimo-jcache_1.0_spec"                       % "1.0-alpha-1",
    "org.apache.hadoop"               % "hadoop-aliyun"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-annotations"                             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-archive-logs"                            % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-archives"                                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-auth"                                    % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-aws"                                     % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-azure"                                   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-azure-datalake"                          % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-client"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-cloud-storage"                           % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-common"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-datajoin"                                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-distcp"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-extras"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-fs2img"                                  % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-gridmix"                                 % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs"                                    % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs-client"                             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs-httpfs"                             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs-native-client"                      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs-nfs"                                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-hdfs-rbf"                                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-kafka"                                   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-kms"                                     % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-app"                    % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-common"                 % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-core"                   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-hs"                     % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-hs-plugins"             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-jobclient"              % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-nativetask"             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-shuffle"                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-client-uploader"               % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-mapreduce-examples"                      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-nfs"                                     % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-openstack"                               % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-ozone-filesystem-hadoop3"                % "1.1.0.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-resourceestimator"                       % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-rumen"                                   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-sls"                                     % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-streaming"                               % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-api"                                % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-applications-distributedshell"      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-applications-unmanaged-am-launcher" % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-client"                             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-common"                             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-registry"                           % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-applicationhistoryservice"   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-common"                      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-nodemanager"                 % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-resourcemanager"             % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-router"                      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-sharedcachemanager"          % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-tests"                       % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-timeline-pluginstorage"      % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-server-web-proxy"                   % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-services-api"                       % "3.1.1.7.1.7.0-551",
    "org.apache.hadoop"               % "hadoop-yarn-services-core"                      % "3.1.1.7.1.7.0-551",
    "org.apache.hive"                 % "hive-beeline"                                   % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-classification"                            % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-cli"                                       % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-common"                                    % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-contrib"                                   % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-druid-handler"                             % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-exec"                                      % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-hbase-handler"                             % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-hplsql"                                    % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-jdbc"                                      % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-jdbc-handler"                              % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-kryo-registrator"                          % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-kudu-handler"                              % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-llap-client"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-llap-common"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-llap-ext-client"                           % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-llap-server"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-llap-tez"                                  % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-metastore"                                 % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-pre-upgrade"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-serde"                                     % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-service"                                   % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-service-rpc"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-shims"                                     % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-standalone-metastore"                      % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-storage-api"                               % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-streaming"                                 % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-testutils"                                 % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "hive-vector-code-gen"                           % "3.1.3000.7.1.7.0-551",
    "org.apache.hive"                 % "kafka-handler"                                  % "3.1.3000.7.1.7.0-551",
    "org.apache.hive.hcatalog"        % "hive-hcatalog-core"                             % "3.1.3000.7.1.7.0-551",
    "org.apache.hive.hcatalog"        % "hive-hcatalog-server-extensions"                % "3.1.3000.7.1.7.0-551",
    "org.apache.hive.shims"           % "hive-shims-common"                              % "3.1.3000.7.1.7.0-551",
    "org.apache.hive.shims"           % "hive-shims-scheduler"                           % "3.1.3000.7.1.7.0-551",
    "org.apache.httpcomponents"       % "httpclient"                                     % "4.5.13",
    "org.apache.httpcomponents"       % "httpcore"                                       % "4.4.13",
    "org.apache.ivy"                  % "ivy"                                            % "2.4.0",
    "org.apache.kafka"                % "kafka-clients"                                  % "2.4.1.7.1.5.0-257",
    "org.apache.kerby"                % "kerb-admin"                                     % "1.1.1",
    "org.apache.kerby"                % "kerb-client"                                    % "1.1.1",
    "org.apache.kerby"                % "kerb-common"                                    % "1.1.1",
    "org.apache.kerby"                % "kerb-core"                                      % "1.1.1",
    "org.apache.kerby"                % "kerb-crypto"                                    % "1.1.1",
    "org.apache.kerby"                % "kerb-identity"                                  % "1.1.1",
    "org.apache.kerby"                % "kerb-server"                                    % "1.1.1",
    "org.apache.kerby"                % "kerb-simplekdc"                                 % "1.1.1",
    "org.apache.kerby"                % "kerb-util"                                      % "1.1.1",
    "org.apache.kerby"                % "kerby-asn1"                                     % "1.1.1",
    "org.apache.kerby"                % "kerby-config"                                   % "1.1.1",
    "org.apache.kerby"                % "kerby-pkix"                                     % "1.1.1",
    "org.apache.kerby"                % "kerby-util"                                     % "1.1.1",
    "org.apache.kerby"                % "kerby-xdr"                                      % "1.1.1",
    "org.apache.kerby"                % "token-provider"                                 % "1.1.1",
    "org.apache.knox"                 % "gateway-cloud-bindings"                         % "1.3.0.7.1.7.0-551",
    "org.apache.knox"                 % "gateway-i18n"                                   % "1.3.0.7.1.7.0-551",
    "org.apache.knox"                 % "gateway-shell"                                  % "1.3.0.7.1.7.0-551",
    "org.apache.knox"                 % "gateway-util-common"                            % "1.3.0.7.1.7.0-551",
    "org.apache.kudu"                 % "kudu-client"                                    % "1.15.0.7.1.7.0-551",
    "org.apache.logging.log4j"        % "log4j-api"                                      % "2.17.0",
    "org.apache.logging.log4j"        % "log4j-core"                                     % "2.17.0",
    "org.apache.logging.log4j"        % "log4j-web"                                      % "2.17.0",
    "org.apache.orc"                  % "orc-core"                                       % "1.5.1.7.1.7.0-551",
    "org.apache.orc"                  % "orc-mapreduce"                                  % "1.5.1.7.1.7.0-551",
    "org.apache.orc"                  % "orc-shims"                                      % "1.5.1.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-avro"                                   % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-cascading"                              % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-cascading3"                             % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-column"                                 % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-common"                                 % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-encoding"                               % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-format-structures"                      % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-generator"                              % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-hadoop"                                 % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-hadoop-bundle"                          % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-jackson"                                % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-pig"                                    % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-pig-bundle"                             % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-protobuf"                               % "1.10.99.7.1.7.0-551",
    "org.apache.parquet"              % "parquet-thrift"                                 % "1.10.99.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-hdfs-plugin-shim"                        % "2.1.0.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-hive-plugin-shim"                        % "2.1.0.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-plugin-classloader"                      % "2.1.0.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-raz-hook-abfs"                           % "2.1.0.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-raz-intg"                                % "2.1.0.7.1.7.0-551",
    "org.apache.ranger"               % "ranger-yarn-plugin-shim"                        % "2.1.0.7.1.7.0-551",
    "org.apache.spark"                %% "spark-avro"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-catalyst"                                % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-core"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-graphx"                                  % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-hadoop-cloud"                            % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-hive"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-kubernetes"                              % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-kvstore"                                 % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-launcher"                                % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-mllib"                                   % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-mllib-local"                             % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-network-common"                          % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-network-shuffle"                         % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-repl"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-sketch"                                  % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-tags"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-unsafe"                                  % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-yarn"                                    % "2.4.7.7.1.7.0-551",
    "org.apache.spark"                %% "spark-sql"                                     % "2.4.7.7.1.7.0-551",
    "org.apache.taglibs"              % "taglibs-standard-impl"                          % "1.2.5",
    "org.apache.taglibs"              % "taglibs-standard-spec"                          % "1.2.5",
    "org.apache.tez"                  % "tez-api"                                        % "0.9.1.7.1.7.0-551",
    "org.apache.tez"                  % "tez-common"                                     % "0.9.1.7.1.7.0-551",
    "org.apache.tez"                  % "tez-runtime-internals"                          % "0.9.1.7.1.7.0-551",
    "org.apache.thrift"               % "libfb303"                                       % "0.9.3",
    "org.apache.thrift"               % "libthrift"                                      % "0.13.0",
    "org.apache.velocity"             % "velocity"                                       % "1.5",
    "org.apache.xbean"                % "xbean-asm7-shaded"                              % "4.13",
    "org.apache.yetus"                % "audience-annotations"                           % "0.5.0",
    "org.apache.zookeeper"            % "zookeeper"                                      % "3.5.5.7.1.7.0-551",
    "org.apache.zookeeper"            % "zookeeper-jute"                                 % "3.5.5.7.1.7.0-551",
    "org.bouncycastle"                % "bcpkix-jdk15on"                                 % "1.60",
    "org.bouncycastle"                % "bcprov-jdk15on"                                 % "1.60",
    "org.checkerframework"            % "checker-compat-qual"                            % "2.5.3",
    "org.checkerframework"            % "checker-qual"                                   % "2.8.1",
    "org.cloudera.logredactor"        % "logredactor"                                    % "2.0.8",
    "org.codehaus.groovy"             % "groovy"                                         % "3.0.7",
    "org.codehaus.groovy"             % "groovy-all"                                     % "2.4.11",
    "org.codehaus.groovy"             % "groovy-console"                                 % "3.0.7",
    "org.codehaus.groovy"             % "groovy-groovysh"                                % "3.0.7",
    "org.codehaus.groovy"             % "groovy-json"                                    % "3.0.7",
    "org.codehaus.groovy"             % "groovy-swing"                                   % "3.0.7",
    "org.codehaus.groovy"             % "groovy-templates"                               % "3.0.7",
    "org.codehaus.groovy"             % "groovy-xml"                                     % "3.0.7",
    "org.codehaus.jackson"            % "jackson-core-asl"                               % "1.9.13-cloudera.1",
    "org.codehaus.jackson"            % "jackson-jaxrs"                                  % "1.9.13",
    "org.codehaus.jackson"            % "jackson-mapper-asl"                             % "1.9.13-cloudera.4",
    "org.codehaus.jackson"            % "jackson-xc"                                     % "1.9.13",
    "org.codehaus.janino"             % "commons-compiler"                               % "3.0.16",
    "org.codehaus.janino"             % "janino"                                         % "3.0.16",
    "org.codehaus.jettison"           % "jettison"                                       % "1.1",
    "org.codehaus.mojo"               % "animal-sniffer-annotations"                     % "1.18",
    "org.codehaus.woodstox"           % "stax2-api"                                      % "3.1.4",
    "org.datanucleus"                 % "datanucleus-api-jdo"                            % "4.2.4",
    "org.datanucleus"                 % "datanucleus-core"                               % "4.1.17",
    "org.datanucleus"                 % "datanucleus-rdbms"                              % "4.1.19",
    "org.datanucleus"                 % "javax.jdo"                                      % "3.2.0-m3",
    "org.eclipse.jetty"               % "apache-jsp"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "apache-jstl"                                    % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-annotations"                              % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-client"                                   % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-http"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-io"                                       % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-jaas"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-jndi"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-plus"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-rewrite"                                  % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-runner"                                   % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-security"                                 % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-server"                                   % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-servlet"                                  % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-util"                                     % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-util-ajax"                                % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-webapp"                                   % "9.4.39.v20210325",
    "org.eclipse.jetty"               % "jetty-xml"                                      % "9.4.39.v20210325",
    "org.eclipse.jetty.toolchain"     % "jetty-schemas"                                  % "3.1.2",
    "org.eclipse.jetty.websocket"     % "websocket-api"                                  % "9.4.39.v20210325",
    "org.eclipse.jetty.websocket"     % "websocket-client"                               % "9.4.39.v20210325",
    "org.eclipse.jetty.websocket"     % "websocket-common"                               % "9.4.39.v20210325",
    "org.eclipse.jetty.websocket"     % "websocket-server"                               % "9.4.39.v20210325",
    "org.eclipse.jetty.websocket"     % "websocket-servlet"                              % "9.4.39.v20210325",
    "org.ehcache"                     % "ehcache"                                        % "3.3.1",
    "org.fusesource.leveldbjni"       % "leveldbjni-all"                                 % "1.8",
    "org.glassfish"                   % "javax.el"                                       % "3.0.0",
    "org.glassfish.hk2"               % "hk2-api"                                        % "2.6.1",
    "org.glassfish.hk2"               % "hk2-locator"                                    % "2.6.1",
    "org.glassfish.hk2"               % "hk2-utils"                                      % "2.6.1",
    "org.glassfish.hk2"               % "osgi-resource-locator"                          % "1.0.3",
    "org.glassfish.hk2.external"      % "aopalliance-repackaged"                         % "2.6.1",
    "org.glassfish.hk2.external"      % "jakarta.inject"                                 % "2.6.1",
    "org.glassfish.jaxb"              % "jaxb-runtime"                                   % "2.3.2",
    "org.glassfish.jersey.containers" % "jersey-container-servlet"                       % "2.32",
    "org.glassfish.jersey.containers" % "jersey-container-servlet-core"                  % "2.32",
    "org.glassfish.jersey.core"       % "jersey-client"                                  % "2.32",
    "org.glassfish.jersey.core"       % "jersey-common"                                  % "2.32",
    "org.glassfish.jersey.core"       % "jersey-server"                                  % "2.32",
    "org.glassfish.jersey.inject"     % "jersey-hk2"                                     % "2.32",
    "org.glassfish.jersey.media"      % "jersey-media-jaxb"                              % "2.32",
    "org.immutables"                  % "gson"                                           % "2.2.4",
    "org.jamon"                       % "jamon-runtime"                                  % "2.3.1",
    "org.javassist"                   % "javassist"                                      % "3.25.0-GA",
    "org.json4s"                      %% "json4s-ast"                                    % "3.5.3",
    "org.json4s"                      %% "json4s-core"                                   % "3.5.3",
    "org.json4s"                      %% "json4s-jackson"                                % "3.5.3",
    "org.json4s"                      %% "json4s-scalap"                                 % "3.5.3",
    "org.jvnet.mimepull"              % "mimepull"                                       % "1.9.3",
    "org.lz4"                         % "lz4-java"                                       % "1.6.0",
    "org.objenesis"                   % "objenesis"                                      % "2.5.1",
    "org.ojalgo"                      % "ojalgo"                                         % "43.0",
    "org.ow2.asm"                     % "asm"                                            % "9.0",
    "org.ow2.asm"                     % "asm-analysis"                                   % "9.0",
    "org.ow2.asm"                     % "asm-commons"                                    % "9.0",
    "org.ow2.asm"                     % "asm-tree"                                       % "9.0",
    "org.postgresql"                  % "postgresql"                                     % "42.2.14",
    "org.reflections"                 % "reflections"                                    % "0.9.10",
    "org.roaringbitmap"               % "RoaringBitmap"                                  % "0.7.45",
    "org.roaringbitmap"               % "shims"                                          % "0.7.45",
    "org.scala-lang"                  % "scala-compiler"                                 % "2.11.12",
    "org.scala-lang"                  % "scala-library"                                  % "2.11.12",
    "org.scala-lang"                  % "scala-reflect"                                  % "2.11.12",
    "org.scala-lang.modules"          %% "scala-parser-combinators"                      % "1.1.0",
    "org.scala-lang.modules"          %% "scala-xml"                                     % "1.0.5",
    "org.scalanlp"                    %% "breeze"                                        % "0.13.2",
    "org.scalanlp"                    %% "breeze-macros"                                 % "0.13.2",
    "org.sklsft.commons"              % "commons-crypto"                                 % "1.0.0",
    "org.slf4j"                       % "jcl-over-slf4j"                                 % "1.7.30",
    "org.slf4j"                       % "jul-to-slf4j"                                   % "1.7.30",
    "org.slf4j"                       % "slf4j-api"                                      % "1.7.30",
    "org.slf4j"                       % "slf4j-log4j12"                                  % "1.7.30",
    "org.spire-math"                  %% "spire"                                         % "0.13.0",
    "org.spire-math"                  %% "spire-macros"                                  % "0.13.0",
    "org.springframework"             % "spring-aop"                                     % "4.3.29.RELEASE",
    "org.springframework"             % "spring-beans"                                   % "4.3.29.RELEASE",
    "org.springframework"             % "spring-context"                                 % "4.3.29.RELEASE",
    "org.springframework"             % "spring-core"                                    % "4.3.29.RELEASE",
    "org.springframework"             % "spring-expression"                              % "4.3.29.RELEASE",
    "org.threeten"                    % "threeten-extra"                                 % "1.5.0",
    "org.tukaani"                     % "xz"                                             % "1.8",
    "org.typelevel"                   %% "machinist"                                     % "0.6.1",
    "org.typelevel"                   %% "macro-compat"                                  % "1.1.1",
    "org.wildfly.openssl"             % "wildfly-openssl"                                % "1.0.7.Final",
    "org.xerial.snappy"               % "snappy-java"                                    % "1.1.7.7",
    "org.yaml"                        % "snakeyaml"                                      % "1.26",
    "oro"                             % "oro"                                            % "2.0.8",
    "sqlline"                         % "sqlline"                                        % "1.3.0",
    "stax"                            % "stax-api"                                       % "1.0.1",
    ("org.apache.avro" % "avro-mapred" % "1.8.2.7.1.7.0-551").classifier("hadoop2"),
    ("org.apache.avro" % "trevni-avro" % "1.8.2.7.1.7.0-551").classifier("hadoop2")
  )
  lazy val akkaActor          = "com.typesafe.akka"            %% "akka-actor"              % versions.akka
  lazy val akkaCluster        = "com.typesafe.akka"            %% "akka-cluster"            % versions.akka
  lazy val akkaClusterTools   = "com.typesafe.akka"            %% "akka-cluster-tools"      % versions.akka
  lazy val akkaContrib        = "com.typesafe.akka"            %% "akka-contrib"            % versions.akka
  lazy val akkaHttp           = "com.typesafe.akka"            %% "akka-http"               % versions.akkaHttp
  lazy val akkaHttpSpray      = "com.typesafe.akka"            %% "akka-http-spray-json"    % versions.akkaHttp
  lazy val akkaKryo           = "com.github.romix.akka"        %% "akka-kryo-serialization" % "0.5.0"
  lazy val akkaRemote         = "com.typesafe.akka"            %% "akka-remote"             % versions.akka
  lazy val akkaSlf4j          = "com.typesafe.akka"            %% "akka-slf4j"              % versions.akka
  lazy val akkaStream         = "com.typesafe.akka"            %% "akka-stream"             % versions.akka
  lazy val akkaStreamTestkit  = "com.typesafe.akka"            %% "akka-stream-testkit"     % versions.akka % Test
  lazy val akkaHttpTestKit    = "com.typesafe.akka"            %% "akka-http-testkit"       % versions.akkaHttp % Test
  lazy val akkaClusterTestKit = "com.typesafe.akka"            %% "akka-multi-node-testkit" % versions.akka % Test
  lazy val akkaTestKit        = "com.typesafe.akka"            %% "akka-testkit"            % versions.akka % Test
  lazy val sttpCore           = "com.softwaremill.sttp.client" %% "core"                    % versions.sttpVersion
  lazy val sttpJson4s         = "com.softwaremill.sttp.client" %% "json4s"                  % versions.sttpVersion
  lazy val json4sJackson      = "org.json4s"                   %% "json4s-jackson"          % versions.json4s
  lazy val mongodbScala       = "org.mongodb.scala"            %% "mongo-scala-driver"      % versions.mongodbScala
  lazy val mongoTest          = "com.github.simplyscala"       %% "scalatest-embedmongo"    % "0.2.4" % Test
  lazy val scalaTest          = "org.scalatest"                %% "scalatest"               % versions.scalaTest % Test
  lazy val allAkka = Seq(
    akkaActor,
    akkaCluster,
    akkaClusterTools,
    akkaContrib,
    akkaHttp,
    akkaHttpSpray,
    akkaKryo,
    akkaRemote,
    akkaSlf4j,
    akkaStream,
    akkaStreamTestkit,
    akkaHttpTestKit,
    akkaClusterTestKit,
    akkaTestKit
  )
  lazy val hbaseClient         = "org.apache.hbase" % "hbase-client" % versions.hbase
  lazy val hbaseCommon         = "org.apache.hbase" % "hbase-common" % versions.hbase
  lazy val hbaseServer         = "org.apache.hbase" % "hbase-server" % versions.hbase
  lazy val hbaseMapreduce      = "org.apache.hbase" % "hbase-mapreduce" % versions.hbase
  lazy val hbaseTestingUtils   = "org.apache.hbase" % "hbase-testing-util" % versions.hbase % Test
  lazy val hbase               = Seq(hbaseClient, hbaseCommon, hbaseServer, hbaseMapreduce)
  lazy val scalaPool           = "io.github.andrebeat" %% "scala-pool" % "0.4.3"
  lazy val darwinCore          = "it.agilelab" %% "darwin-core" % versions.darwin
  lazy val darwinMockConnector = "it.agilelab" %% "darwin-mock-connector" % versions.darwin
  lazy val avro4sCore = "com.sksamuel.avro4s" % "avro4s-core_2.11" % versions.avro4sVersion excludeAll ExclusionRule(
    "org.json4s"
  )
  lazy val avro4sJson = "com.sksamuel.avro4s" % "avro4s-json_2.11" % versions.avro4sVersion excludeAll ExclusionRule(
    "org.json4s"
  )
  lazy val avro4s             = Seq(avro4sCore, avro4sJson)
  lazy val swaggerCore        = "io.swagger.core.v3" % "swagger-core" % "2.1.2"
  lazy val nameOf             = "com.github.dwickern" %% "scala-nameof" % "1.0.3" % "provided"
  lazy val sparkYarn          = parcelDependencies.find(x => x.name == "spark-yarn").get
  lazy val guava              = parcelDependencies.find(x => x.name == "guava").get
  lazy val sparkCore          = parcelDependencies.find(x => x.name == "spark-core").get
  lazy val parserCombinators  = parcelDependencies.find(x => x.name.contains("scala-parser-combinators")).get
  lazy val postgresqlEmbedded = "com.opentable.components" % "otj-pg-embedded" % versions.postgresqlEmbeddedVersion % Test
  lazy val mockOkHttp2        = "com.squareup.okhttp" % "mockwebserver" % "2.7.5" % Test // in sync with CDP
  lazy val okHttp2 = Seq(
    "com.squareup.okhttp" % "mockwebserver" % "2.7.5" % Test,
    "com.squareup.okhttp" % "mockwebserver" % "2.7.5"
  )
  lazy val kafkaClients       = parcelDependencies.find(x => x.name == "kafka-clients").get
  lazy val sparkCatalyst      = parcelDependencies.find(x => x.name == "spark-catalyst").get
  lazy val sparkCatalystTests = sparkCatalyst % Test classifier "tests"
  lazy val sparkSql           = parcelDependencies.find(x => x.name == "spark-sql").get
  lazy val sparkCoreTests     = sparkCore % Test classifier "tests"
  lazy val sparkSQLTests      = sparkSql % Test classifier "tests"
  lazy val sparkTags          = parcelDependencies.find(x => x.name == "spark-tags").get
  lazy val sparkTagsTests     = sparkTags % Test classifier "tests"
  lazy val kms                = parcelDependencies.find(x => x.name == "hadoop-kms").get
  lazy val kmsTestModule      = kms.classifier("tests") % "test"
  lazy val elasticSearch      = "org.elasticsearch" % "elasticsearch" % versions.elasticSearch
  lazy val elasticSearchSpark = "org.elasticsearch" %% "elasticsearch-spark-20" % versions.elasticSearchSpark

  override lazy val scalaCompilerDependencies: Seq[sbt.ModuleID] = Seq(scalaPool) ++ testDependencies
  override lazy val modelDependencies
      : Seq[sbt.ModuleID] = parcelDependencies ++ allAkka ++ testDependencies :+ mongoTest :+ mongodbScala
  override lazy val coreDependencies
      : Seq[sbt.ModuleID]                                             = Seq(darwinCore, darwinMockConnector) ++ avro4s ++ testDependencies
  override lazy val testDependencies: Seq[sbt.ModuleID]               = Seq(scalaTest)
  override lazy val scalaTestDependencies: Seq[sbt.ModuleID]          = testDependencies
  override lazy val repositoryCoreDependencies: Seq[sbt.ModuleID]     = testDependencies ++ Seq(shapeless)
  override lazy val repositoryMongoDependencies: Seq[sbt.ModuleID]    = Seq(nameOf, mongoTest) ++ testDependencies ++ Seq(shapeless)
  override lazy val repositoryPostgresDependencies: Seq[sbt.ModuleID] = Seq(postgresqlEmbedded) ++ testDependencies
  override lazy val masterDependencies: Seq[sbt.ModuleID]             = testDependencies ++ allAkka
  override lazy val producersDependencies: Seq[sbt.ModuleID]          = testDependencies ++ allAkka
  override lazy val consumersSparkDependencies
      : Seq[sbt.ModuleID]                                      = Seq(quartz, nameOf) ++ wireMock ++ hbase ++ avro4sTestAndDarwin ++ testDependencies ++ allAkka ++ avro4sTestAndDarwin
  override lazy val  pluginElasticSparkDependencies
      : Seq[sbt.ModuleID]                                           = Seq(elasticSearch, elasticSearchSpark) ++ testDependencies
  override lazy val pluginHbaseSparkDependencies: Seq[sbt.ModuleID] = testDependencies
  override lazy val pluginPlainHbaseWriterSparkDependencies: Seq[sbt.ModuleID] = hbase ++ testDependencies ++ Seq(
    hbaseTestingUtils
  )
  override lazy val pluginKafkaSparkDependencies: Seq[sbt.ModuleID]    = Seq(spark_sql_kafka) ++ testDependencies
  override lazy val pluginKafkaSparkOldDependencies: Seq[sbt.ModuleID] = Seq(spark_sql_kafka_old) ++ testDependencies
  override lazy val pluginSolrSparkDependencies: Seq[sbt.ModuleID]     = Seq(sparkSolr, solrj) ++ testDependencies
  override lazy val pluginMongoSparkDependencies: Seq[sbt.ModuleID] = Seq(
    ("org.mongodb.spark" %% "mongo-spark-connector" % "2.4.3").exclude("org.mongodb", "mongo-java-driver"),
    "org.mongodb" % "mongo-java-driver" % "3.12.2"
  )
  override lazy val pluginMailerSparkDependencies: Seq[sbt.ModuleID]        = Seq(javaxMail) ++ testDependencies
  override lazy val pluginHttpSparkDependencies: Seq[sbt.ModuleID]          = wireMock ++ testDependencies ++ okHttp2
  override lazy val pluginCdcSparkDependencies: Seq[sbt.ModuleID]           = Seq(delta) ++ testDependencies
  override lazy val microserviceCatalogDependencies: Seq[sbt.ModuleID]      = testDependencies
  override lazy val pluginParallelWriteSparkDependencies: Seq[sbt.ModuleID] = Seq(delta) ++ testDependencies ++ okHttp2
  override lazy val yarnAuthHdfsDependencies: Seq[ModuleID]                 = Seq(sparkYarn) ++ testDependencies
  override lazy val yarnAuthHBaseDependencies
      : Seq[ModuleID] = Seq(sparkYarn, hbaseServer, hbaseCommon) ++ testDependencies
  override lazy val sparkTelemetryPluginDependencies: Seq[sbt.ModuleID] =
    Seq(guava, sparkCore, parserCombinators, kafkaClients) ++ testDependencies
  override lazy val sparkNifiPluginDependencies: Seq[sbt.ModuleID] =
    parcelDependencies.find(x => x.name.contains("spark.core")).toList ++ testDependencies
  override lazy val nifiStatelessDependencies: Seq[sbt.ModuleID] = Seq(
    "org.apache.nifi" % "nifi-stateless" % versions.nifi % "provided"
  ) ++ testDependencies
  override lazy val nifiClientDependencies: Seq[sbt.ModuleID] =
    testDependencies :+
      akkaHttp :+
      akkaHttpSpray :+
      sttpCore :+
      sttpJson4s :+
      json4sJackson
  lazy val quartz                                                          = "org.quartz-scheduler" % "quartz" % versions.quartz
  override lazy val whitelabelModelsDependencies: Seq[sbt.ModuleID]        = testDependencies
  override lazy val whitelabelMasterDependencies: Seq[sbt.ModuleID]        = testDependencies
  override lazy val whitelabelProducerDependencies: Seq[sbt.ModuleID]      = testDependencies
  override lazy val whitelabelSparkConsumerDependencies: Seq[sbt.ModuleID] = testDependencies
  override lazy val openapiDependencies: Seq[sbt.ModuleID]                 = Seq(swaggerCore)
  override lazy val kmsTest: Seq[Def.Setting[_]] = Seq(
    (Test / transitiveClassifiers) := Seq(Artifact.TestsClassifier, Artifact.SourceClassifier),
    libraryDependencies ++= Seq(kms, kmsTestModule)
  )
  override lazy val sparkPluginBasicDependencies: Seq[sbt.ModuleID] = Seq.empty
  override lazy val whitelabelMasterScriptClasspath                 = scriptClasspath += ""
  override lazy val whitelabelProducerScriptClasspath               = scriptClasspath += ""
  override lazy val whitelabelSparkConsumerScriptClasspath          = scriptClasspath += ":$HADOOP_CONF_DIR:$HBASE_CONF_DIR"
  override lazy val whiteLabelConsumersRtScriptClasspath            = scriptClasspath += ""
  override lazy val whiteLabelSingleNodeScriptClasspath             = scriptClasspath += ""
  lazy val avro4sTest                                               = avro4s.map(_ % Test)
  lazy val avro4sTestAndDarwin                                      = avro4sTest ++ Seq(darwinMockConnector % Test)
  val wireMock =
    Seq("com.github.tomakehurst" % "wiremock-standalone" % "2.25.0" % Test, "xmlunit" % "xmlunit" % "1.6" % Test)

  override val awsAuth: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-aws" % versions.hadoop,
    "org.apache.hadoop" % "hadoop-common" % versions.hadoop,
    "com.amazonaws"%"aws-java-sdk-bundle" % versions.awsBundle force()
  )
}
