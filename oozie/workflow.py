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
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        with tag('workflow-app', ('xmlns:sla', 'uri:oozie:sla:0.2'), name=self.name, xmlns="uri:oozie:workflow:0.5"):
            doc.stag('start', to=self.actions[0].name)
            for index, action in enumerate(self.actions):
                with tag("action", name=action.name):
                    doc.asis(action.as_xml(indent))
                    if index + 1 < len(self.actions):
                        next_action = self.actions[index+1]
                        doc.stag("ok", to=next_action.name)
                    else: 
                        doc.stag("ok", to="end")
                    doc.stag("error", to="kill")

            with tag("kill", name="kill"):
                with tag("message"):
                    text("KIA")
            doc.stag('end', name="end")

        return indent(doc.getvalue())

