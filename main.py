from sys import argv
from oozie import OozieServer, Workflow, actions, coordinators

if __name__ == "__main__":
    serv = OozieServer.OozieServer(argv[1])
    jerb = Workflow.Workflow("fooer")
    cord = coordinators.Coordinator("blah", jerb, 1440)
    jerb.add(actions.ShellAction(name="extract", command='ls', env=[]))
    jerb.add(actions.ShellAction(name="build", command='ls', env=[]))
    jerb.add(actions.ShellAction(name="resolve", command='whoami', env=[]))
    jerb.add(actions.ShellAction(name="load1", command='whoami', env=[]))
    jerb.add(actions.ShellAction(name="load2", command='whoami', env=[]))

    #this guy defines the coordinator job, probably
    f = open("coordinator.xml", "w")
    f.write(cord.as_xml(True))
   
    #upload to hdfs
    path = ""
    #now submit to oozie and maybe magic
    serv.submit(path)  

