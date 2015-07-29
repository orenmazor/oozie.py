from yattag import Doc, indent
import os


class SubWorkflowAction:
    def __init__(self, name, flow):
        self.name = name
        self.sub_wf_path = flow

    def as_xml(self, indentation=False):
        doc, tag, text = Doc().tagtext()
        with tag('sub-workflow'):
            with tag('app-path'):
                text(self.sub_wf_path)
            doc.stag("propagate-configuration")

        xml = doc.getvalue()
        if indentation:
            return indent(xml)
        else:
            return xml


class ShellAction:
    def __init__(self, name, command, env, archives=[], args=[], files=[]):
        self.name = name
        self.command = command
        self.environment_vars = env
        self.arguments = args
        self.files = files
        self.archives = archives

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
            for archive in self.archives:
                with tag('archive'):
                    text(archive)
            for f in self.files:
                with tag('file'):
                    text(f)

        xml = doc.getvalue()
        if indentation:
            return indent(xml)
        else:
            return xml

