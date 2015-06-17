from requests import post, get
from json import loads
from yattag import Doc
from sys import argv
from sortedcontainers import SortedList

class Oozie:
    def __init__(self, url):
        self.url = url

        print "Connected to Ooozie Version {0}".format(self.version())

    def version(self):
        response = get("{0}/oozie/v1/admin/build-version".format(self.url))
        if response.status_code != 200:
            raise RuntimeError("fuck")
        else:
            return loads(response.content)["buildVersion"]

    def submit_flow(self, job):
        pass


class Workflow:
    def __init__(self, name):
        self.name = name
        self.actions = SortedList()

    def add(self, action):
        self.actions.append(action)

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        with tag('workflow-app', xmlns="uri:oozie:workflow:0.4", name=self.name):
            doc.stag('start', to=self.name)

            for action in self.actions:
                with tag("action", name=action.name):
                    doc.asis(action.as_xml())

        return doc.getvalue()

class ShellAction:
    def __init__(self, name, command, env, args=[]):
        self.name = name
        self.command = command
        self.environment_vars = env
        self.arguments = []

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        with tag('shell', xmlns="uri:oozie:shell-action:0.2"):
            with tag('exec'):
                text(self.command)
            for argument in self.arguments:
                with tag('argument'):
                    text(argument)
            for env in self.environment_vars:
                with tag('env-var'):
                    text(env)
            doc.stag('ok', to=
            doc.stag('capture-output')

        return doc.getvalue()




if __name__ == "__main__":
    serv = Oozie(argv[1])
    jerb = Workflow("fooer")
    jerb.add(ShellAction(name="build", command='ls', env=[]))
    print jerb.as_xml()

