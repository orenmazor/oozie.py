from yattag import Doc, indent
from dateutil.relativedelta import relativedelta
from datetime import datetime

class Coordinator:
  def __init__(self, name, workflow, frequency):
    self.name = name
    self.workflow = workflow
    self.frequency = frequency
    self.start = datetime.now().strftime("%Y-%m-%dT%H:%MZ")
    self.end = (datetime.now() + relativedelta(years=100)).strftime("%Y-%m-%dT%H:%MZ")

  def as_xml(self, wf_path):
    doc, tag, text = Doc().tagtext()
    doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
    with tag("coordinator-app", xmlns="uri:oozie:coordinator:0.2", timezone="UTC", name=self.name, start=self.start, end=self.end, frequency=str(self.frequency)):
      with tag("controls"):
        with tag("timeout"):
          text("600")
        with tag("concurrency"):
          text("1")
        with tag("execution"):
          text("FIFO")
        with tag("throttle"):
          text("1")

      with tag("action"):
        with tag("workflow"):
          with tag("app-path"):
            text(wf_path)

    return indent(doc.getvalue())
