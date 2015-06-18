from sys import argv
from oozie import oozie_server, workflow, actions, coordinators

if __name__ == "__main__":
    serv = oozie_server.OozieServer(argv[1])
    jerb = workflow.Workflow("fooer")
    cord = coordinators.Coordinator("blah", jerb, 1440)
    jerb.add(actions.ShellAction(name="resolve", command='whoami', env=[]))
    jerb.add(actions.ShellAction(name="resolvetwo", command='ls', env=[]))

    serv.submit(cord)  

