# Goal

The purpose of this is to have a cleaner, non-xml involving DSL for generating coordinated workflows for oozie.

# Usage

Right now you'll need to make these environment variable settings:

```
NAMENODE=hdfs://zombo-production:8032
JOBTRACKER=hadoop-rm.zombo.com
WEBHDFS_HOST=hadoop-misc.zombo.com
HADOOP_PRODUCTION=zombo-production
```

```
    a_server = oozie_server.OozieServer(argv[1])
    a_job = workflow.Workflow("fooer")
    a_coordinator = coordinators.Coordinator("blah", jerb, 1440)
    a_job.add(actions.ShellAction(name="first_action", command='whoami', env=[]))
    a_job.add(actions.ShellAction(name="another_action", command='ls', env=[]))

    a_server.submit(a_coordinator)  
```
