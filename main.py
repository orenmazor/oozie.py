from sys import argv
from oozie import OozieServer, Workflow, actions

if __name__ == "__main__":
    serv = OozieServer.OozieServer(argv[1])
    jerb = Workflow.Workflow("fooer")
    jerb.add(actions.ShellAction(name="build", command='ls', env=[]))
    jerb.add(actions.ShellAction(name="resolve", command='whoami', env=[]))
    print jerb.as_xml(True)

    serv.submit(jerb)  

