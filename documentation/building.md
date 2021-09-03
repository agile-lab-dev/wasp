#### Building WASP

Wasp is built and tested under different flavours:

| FLAVOR | Spark | Scala | Hadoop | Transitive dependencies | postfix |
| ---- | ----  | ----  | ---- | ---- | ---- |
| VANILLA2 | 2.4.7 | 2.11 | 2.x | ❌ | None |
| CDH6 | 2.4cdh6.3.2 | 2.11 | 3.0.0cdh6.3.2 | ✅ | cdh6 |

Flavors should change the artifact version (or the artifactId) so that they can all be published without conflicts. 
For that reason, the "postfix" of the flavor is appended to the version of the artifact (before the SNAPSHOT qualifier, if present).

Wasp artifacts are published in all different flavors, the versions distinguishes them, for example,
to resolve the default (i.e. VANILLA2) dependencies of wasp-consumers-spark, you will need the following
sbt ModuleID:

```
"it.agilelab" %% "wasp-consumers-spark" % "2.27.0-486-vanilla-build-of-wasp-SNAPSHOT"
```

while to resolve the cdh6 flavor:

```
"it.agilelab" %% "wasp-consumers-spark" % "2.27.0-486-vanilla-build-of-wasp-cdh6-SNAPSHOT"
```

Note the added cdh6 post-fix in the version, before the (optional) `SNAPSHOT`.

You can find all the supported flavours under `project/Flavor.scala`, it's a sealed hierarchy of case objects.
The flavour is defined at sbt startup through the environment variable `WASP_FLAVOR` or the JVM system property `WASP_FLAVOR` (environment variable takes precedence over system property), this means that in order to build Wasp in two different flavours, you will need to run two different instances of SBT. This choice was made to limit the number of keys present in the SBT build (which is already around 51k).

## Transitive dependencies

If a flavor is marked as one that brings transitive dependencies, it will bring in your project all
the needed dependencies as soon as you import a Wasp artifact. This is really convenient if you are
working in the *exact* same environment as the build flavor (i.e. cdh 6.3.2 for cdh6).

Flavors that do not bring transitive depedncies will include only dependencies that are not part of
an Hadoop distribution, therefore will give you less headaches with regard to dependency exclusions.

The bottom line is: if you find the correct flavor, pick it, otherwise go with the vanilla flavor and
add your runtime dependencies by yourself.

---

In order to run an SBT instance selecting a flavour which is not the default, from a shell (it has been tested with `bash`, `fish`, `zsh` and `sh`) type:

```
WASP_FLAVOR=CDH6 sbt
```

In order to make IntelliJ Idea load the project in a particular flavour, after the initial import (which will happen in the default flavour),

Click on the tool icon of the SBT panel of Intellij, then on `sbt Setting...`:

![Select sbt Settings](building/select_sbt_settings.png)

Then on the VM Parameters text box enter: `-DWASP_FLAVOR=<the flavour of your choice>`

![Set VM parameters](building/set_vm_parameters.png)

After that, proceed to apply the changes and then reload the sbt shell and re-import the project:

![Restart and refresh](building/restart_and_refresh.png)

You will notice in the logs of the sbt shell a log similar to this:

```
*****************************
* Building for flavor: CDH6 *
*****************************
```

That should feature the flavour you selected.