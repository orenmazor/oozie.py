from yattag import Doc, indent

class Coordinator:
  def __init__(self, name, workflow, frequency):
    self.name = name
    self.workflow = workflow
    self.frequency = frequency

  def as_xml(self, indentation=False):
    doc, tag, text = Doc().tagtext()
    with tag("coordinator-app", name=self.name, frequency=str(self.frequency)):
      doc.asis(self.workflow.as_xml(indent))

    xml = doc.getvalue()
    if indentation:
        return indent(xml)
    else:
        return xml
