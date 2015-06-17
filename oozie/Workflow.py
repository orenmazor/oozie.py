from yattag import Doc, indent
from sortedcontainers import SortedList

class Workflow:
    def __init__(self, name, queue="default"):
        self.name = name
        self.actions = SortedList()
        self.queue = queue

    def add(self, action):
        self.actions.append(action)

    def as_xml(self, indentation=False):
        doc, tag, text = Doc().tagtext()
        with tag('workflow-app', xmlns="uri:oozie:workflow:0.4", name=self.name):
            with tag('global'):
              with tag('configuration'):
                with tag('property'):
                  with tag('name'):
                    text('queueName')
                  with tag('value'):
                    text(self.queue)

            doc.stag('start', to=self.name)

            for index, action in enumerate(self.actions):
                with tag("action", name=action.name):
                    doc.asis(action.as_xml(indent))
                    if index + 1 < len(self.actions):
                      next_action = self.actions[index+1]
                      doc.stag("ok", to=next_action.name)
                    else: 
                      doc.stag("ok", to="success")
                    doc.stag("error", to="fail")

        xml = doc.getvalue()
        if indentation:
          return indent(xml)
        else:
          return xml

