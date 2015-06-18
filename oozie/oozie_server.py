from requests import get, post
import os
from time import time
from yattag import Doc
from json import loads
from pywebhdfs.webhdfs import PyWebHdfsClient


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

    def clear(self, coordinator):
        #TODO find previous instance of this coordinator
        pass

    def resubmit(self, coordinator):
        self.clear(coordinator.name)
        self.submit(coordinator)

    def submit(self, coordinator):
        deployment_path = "user/oozie/coordinators/{0}/{1}".format(time(), coordinator.name)
        workflow_path = "{0}/workflow.xml".format(deployment_path)
        coordinator_path = "{0}/coordinator.xml".format(deployment_path)
        hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')
        hdfs.make_dir(deployment_path)
        hdfs.create_file(coordinator_path, coordinator.as_xml("/"+workflow_path))
        hdfs.create_file(workflow_path, coordinator.workflow.as_xml())

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
                    text("/"+deployment_path)

        configuration = doc.getvalue()
        response = post("{0}/oozie/v1/jobs".format(self.url), data=configuration, headers={'Content-Type': 'application/xml'})

        if response.status_code > 399:
            print response.headers["oozie-error-message"]
        print response.status_code
        print response.content
