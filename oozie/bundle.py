from yattag import Doc, indent


class Bundle:
    def __init__(self, name, queue="default"):
        self.name = name
        self.actions = ()
        self.queue = queue

    def add(self, action):
        self.actions = self.actions + (action,)

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        with tag('bundle-app', name=self.name, xmlns="uri:oozie:bundle:0.1"):
            for index, coordinator in enumerate(self.coordinators):
                with tag("coordinator", name=coordinator.name):
                    with tag("app-path"):
                        coordinator.path()

        return indent(doc.getvalue())

