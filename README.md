# Usage

NAMENODE=hdfs://??????:???? 
JOBTRACKER=????:??? 
WEBHDFS_HOST=???? 
HADOOP_PRODUCTION=??????? 
python main.py http://??????:11000

```
    serv = oozie_server.OozieServer(argv[1])
    jerb = workflow.Workflow("fooer")
    cord = coordinators.Coordinator("blah", jerb, 1440)
    jerb.add(actions.ShellAction(name="first_action", command='whoami', env=[]))
    jerb.add(actions.ShellAction(name="another_action", command='ls', env=[]))

    serv.submit(cord)  
```
