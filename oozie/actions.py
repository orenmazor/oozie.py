from yattag import Doc, indent
import os

class ShellAction:
    def __init__(self, name, command, env, args=[]):
        self.name = name
        self.command = command
        self.environment_vars = env
        self.arguments = args

    def as_xml(self, indentation=False):
        doc, tag, text = Doc().tagtext()
        with tag('shell', xmlns="uri:oozie:shell-action:0.2"):
            #do we actually need these even if we dont use them?
            with tag('job-tracker'):
                text(os.environ["JOBTRACKER"])
            with tag('name-node'):
                text(os.environ["NAMENODE"])

            with tag('exec'):
                text(self.command)
            for argument in self.arguments:
                with tag('argument'):
                    text(argument)
            for env in self.environment_vars:
                with tag('env-var'):
                    text(env)
            doc.stag('capture-output')

        xml = doc.getvalue()
        if indentation:
          return indent(xml)
        else:
          return xml

