from yattag import Doc, indent


class Bundle:
    def __init__(self, name):
        self.name = name
        self.coordinators = ()

    def add(self, coordinator, path):
        self.coordinators = self.coordinators + ((coordinator, path),)

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        with tag('bundle-app', name=self.name, xmlns="uri:oozie:bundle:0.1"):
            for index, coordinator in enumerate(self.coordinators):
                with tag("coordinator", name=coordinator[0].name):
                    with tag("app-path"):
                        coordinator[1]

        return indent(doc.getvalue())

