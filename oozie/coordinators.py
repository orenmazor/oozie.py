from yattag import Doc, indent
from pywebhdfs.webhdfs import PyWebHdfsClient
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime

class Coordinator:
  def __init__(self, name, workflow, frequency, starttime, endtime=None, path="user/oozie/coordinators"):
    self.name = name
    self.workflow = workflow
    self.frequency = frequency
    self.start = starttime  # datetime.now().strftime("%Y-%m-%dT%H:%MZ")
    if endtime:
        self.end = endtime
    else:
        self.end = (datetime.now() + relativedelta(years=100)).strftime("%Y-%m-%dT%H:%MZ")
    self.path = path

  def path(self):
    return self.path

  def save(self):
    hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')
    coordinator_path = "{0}/{1}/coordinator.xml".format(self.path, self.name)
    hdfs.make_dir(self.path)
    hdfs.create_file(coordinator_path, self.as_xml())

  def as_xml(self):
    doc, tag, text = Doc().tagtext()
    doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
    with tag("coordinator-app", xmlns="uri:oozie:coordinator:0.2", timezone="America/New_York", name=self.name, start=self.start, end=self.end, frequency=str(self.frequency)):
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
            text(self.workflow.path)

    return indent(doc.getvalue())
