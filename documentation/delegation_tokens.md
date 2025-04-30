# Delegation Tokens Renewal in spark 3.x

The custom mechanism of wasp to obtain the delegation token HBase and HDFS is no longer necessary.
You can use the Spark feature for delegation tokens. Following the [official Spark documentation](https://archive.apache.org/dist/spark/docs/3.4.1/security.html#kerberos),
applications that use WASP will be able to benefit from this Spark feature.


# Delegation Tokens Renewal in spark 2.x

For spark 2.x Wasp rolled its own delegation token providers, using the following configurations:

**This implementation has been removed because it's not needed on Spark 3.x**

```aiignore
 
    #disable builtin hbase provider
	spark.yarn.security.credentials.hbase.enabled = false 
    #disable builtin hdfs provider
    spark.yarn.security.credentials.hadoopfs.enabled = false 
    #disable caching of FileSystem instances by hadoop code (it would cache expired tokens)
	spark.hadoop.fs.hdfs.impl.disable.cache = true 
    #am needs to know the principal
	spark.yarn.principal = "andrea.fonti@CLUSTER01.ATSCOM.IT"
    #am needs a keytab
    spark.yarn.keytab = "andrea.fonti.2.keytab"
    #how often am should check for renewal
	spark.yarn.credentials.renewalTime = "10000"
    #how often executors and driver should check for renewal
    spark.yarn.credentials.updateTime = "10000"
    #distribute keytab
    spark.yarn.dist.files = "file:///root/configurations/andrea.fonti.keytab" 
    #force spark to authenticate
    spark.authenticate = "true"
    #hadoop file system to access (pipe separated uris)
    spark.wasp.yarn.security.tokens.hdfs.fs.uris = "hdfs://nameservice1/user/andrea.fonti"
```

For more info, look at [RELEASE NOTES.md](../RELEASE%20NOTES.md).