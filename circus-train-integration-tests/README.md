## Running the integration tests

Generally the integration tests should "just work" when run via Maven on the command line and in an IDE. However we have seen issues where 
an IDE may struggle with the shading and relocating of certain classes which in turn causes certain tests to fail. This manifests itself in 
tests that pass on the command line but fail in the IDE, usually with a failure like
```
java.util.ServiceConfigurationError: org.jclouds.providers.ProviderMetadata: Provider org.jclouds.azureblob.AzureBlobProviderMetadata could not be instantiated
	at java.util.ServiceLoader.fail(ServiceLoader.java:232)
	at java.util.ServiceLoader.access$100(ServiceLoader.java:185)
	...
Caused by: java.lang.NoClassDefFoundError: com/google/common/base/MoreObjects$ToStringHelper
    ...
Caused by: java.lang.ClassNotFoundException: com.google.common.base.MoreObjects$ToStringHelper
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:352)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)    
```
This indicates that the shading and relocation (in the above example of a Guava class) hasn't been activated in the IDE. One solution to this is to 
rebuild the entire project using Maven on the command line and then re-run the tests in the IDE. The following steps should achieve that: 

1. Run `mvn -DskipTests install` in the top-level Circus Train parent project (this will shade and relocate classes using the Maven 
plugin into the packaged jar artifacts).
2. Refresh the project in the IDE (this will get the IDE to pick up the above classes via their artifacts).
3. Close the `circus-train-common-test` module project - this will get the IDE to use the jar file for this which contains the shaded classes 
   instead of the source code directly.
4. Run the integration tests (which should now pass!)
