from yattag import Doc, indent

class Workflow:
    def __init__(self, name, queue="default"):
        self.name = name
        self.actions = ()
        self.queue = queue

    def add(self, action):
        self.actions = self.actions + (action,)

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        with tag('workflow-app', xmlns="uri:oozie:workflow:0.4", name=self.name):
            with tag('global'):
              with tag('configuration'):
                with tag('property'):
                  with tag('name'):
                    text('queueName')
                  with tag('value'):
                    text(self.queue)

            doc.stag('start', to=self.actions[0].name)
            doc.stag('end', name=self.actions[-1].name)
            doc.stag('join')
            doc.stag('decision')
            doc.stag('fork')

            for index, action in enumerate(self.actions):
              with tag("action", xmlns="uri:oozie:workflow:0.4", name=action.name):
                    doc.asis(action.as_xml(indent))
                    if index + 1 < len(self.actions):
                      next_action = self.actions[index+1]
                      doc.stag("ok", to=next_action.name)
                    else: 
                      doc.stag("ok", to="success")
                    doc.stag("error", to="fail")
            with tag("kill", name="killAction"):
              with tag("message"):
                text("KIA")


        return indent(doc.getvalue())

