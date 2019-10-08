---
name: Bug report
about: Create a report to help us improve

---
<!-- 
 Before raising a bug report please consider the following:
   1. If you want to ask a question don't raise a bug report - rather use the mailing list at https://groups.google.com/forum/#!forum/circus-train-user
   2. Please ensure that the bug your are reporting is actually in Circus Train and not with Hive or the underlying infrastructure being used to perform the replication. 
-->
**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behaviour ideally including the configuration files you are using (feel free to rename any sensitive information like server and table names etc.)

**Expected behavior**
A clear and concise description of what you expected to happen.

**Logs**
Please add the log output from Circus Train when the error occurs, full stack traces are especially useful. It might also be worth looking in any Hadoop cluster log files (if you're performing a distributed copy) to see if anything is output there and including that too.

**Versions (please complete the following information):**
- Circus Train Version: 
- Hive Versions: for whatever Hive client libraries you are using as well as the versions of the Hive metastore services that you are replicating from and to.

**Additional context**
Add any other context about the problem here.
