from requests import get, post
import os
from yattag import Doc, indent
from json import loads

class OozieServer():
    def __init__(self, url):
        self.url = url

        print "Connected to Ooozie Version {0}".format(self.version())

    def version(self):
        response = get("{0}/oozie/v1/admin/build-version".format(self.url))
        if response.status_code != 200:
            raise RuntimeError("fuck")
        else:
            return loads(response.content)["buildVersion"]

    def submit(self, path):
      doc, tag, text = Doc().tagtext()
      with tag("configuration"):
        with tag("property"):
          with tag("name"):
            text("user.name")
          with tag("value"):
            text("oozie")

        with tag("property"):
          with tag("name"):
            text("oozie.coord.application.path")
          with tag("value"):
            text(os.environ["HADOOP_PRODUCTION"] + "/oozie/coord/" + path)

      content = doc.getvalue()
      print content
      response = post("{0}/oozie/v1/jobs".format(self.url), data=content, headers={'Content-Type': 'application/xml'})

