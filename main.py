from sys import argv
from oozie import OozieServer, Workflow, actions, coordinators

if __name__ == "__main__":
    serv = OozieServer.OozieServer(argv[1])
    jerb = Workflow.Workflow("fooer")
    cord = coordinators.Coordinator("blah", jerb, 1440)
    jerb.add(actions.ShellAction(name="build", command='ls', env=[]))
    jerb.add(actions.ShellAction(name="resolve", command='whoami', env=[]))
    print cord.as_xml(True)

    serv.submit(cord)  

