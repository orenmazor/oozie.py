from yattag import Doc, indent


class Bundle:
    def __init__(self, name):
        self.name = name
        self.coordinators = ()

    def add(self, coordinator):
        self.coordinators = self.coordinators + (coordinator,)

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        with tag('bundle-app', name=self.name, xmlns="uri:oozie:bundle:0.1"):
            for coordinator in self.coordinators:
                with tag("coordinator", name=coordinator.name):
                    with tag("app-path"):
                        text(coordinator.path + "/" + coordinator.name)

        return indent(doc.getvalue())

