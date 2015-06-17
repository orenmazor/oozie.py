from yattag import Doc, indent
from dateutil.relativedelta import relativedelta
from datetime import datetime

class Coordinator:
  def __init__(self, name, workflow, frequency):
    self.name = name
    self.workflow = workflow
    self.frequency = frequency
    self.start = datetime.now().isoformat()
    self.end = (datetime.now() + relativedelta(years=100)).isoformat()

  def as_xml(self, indentation=False):
    doc, tag, text = Doc().tagtext()
    with tag("coordinator-app", name=self.name, start=self.start, end=self.end, frequency=str(self.frequency)):
      with tag("controls"):
        with tag("timeout"):
          text("600")
        with tag("concurrency"):
          text("1")
        with tag("execution"):
          text("FIFO")
        with tag("throttle"):
          text("1")
      doc.asis(self.workflow.as_xml(indent))

    xml = doc.getvalue()
    if indentation:
        return indent(xml)
    else:
        return xml
