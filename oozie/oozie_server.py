from requests import get, post
import os
from time import time
from yattag import Doc
from json import loads
from pywebhdfs.webhdfs import PyWebHdfsClient
from oozie import bundle


class OozieServer():
    def __init__(self, url):
        self.url = url

        print "Connected to Ooozie Version {0}".format(self.version())

    def version(self):
        response = get("{0}/oozie/v1/admin/build-version".format(self.url))
        if response.status_code != 200:
            raise RuntimeError("Couldn't reach oozie server. Is the provided url correct?")
        else:
            return loads(response.content)["buildVersion"]

    def submit(self, bundle_name, coords, files=[]):
        hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')
        deployment_path = "user/oozie/coordinators/{0}".format(time())
        bundle_path = "{0}/bundle.xml".format(deployment_path)
        bund = bundle.Bundle(bundle_name)

        for coordinator in coords:
            workflow_path = "{0}/{1}/workflow.xml".format(deployment_path, coordinator.name)
            coordinator_path = "{0}/{1}/coordinator.xml".format(deployment_path, coordinator.name)
            hdfs.make_dir(deployment_path)
            hdfs.create_file(coordinator_path, coordinator.as_xml("/"+workflow_path))
            hdfs.create_file(workflow_path, coordinator.workflow.as_xml())
            bund.add(coordinator, "/"+coordinator_path)
        
        hdfs.create_file(bundle_path, bund.as_xml())

        for f in files:
            hdfs.create_file("{}/{}".format(deployment_path, f.name), f.read())  

        doc, tag, text = Doc().tagtext()
        with tag("configuration"):
            with tag("property"):
                with tag("name"):
                    text("user.name")
                with tag("value"):
                    text("oozie")

            with tag("property"):
                with tag("name"):
                    text("oozie.bundle.application.path")
                with tag("value"):
                    text("/"+deployment_path)

        configuration = doc.getvalue()
        response = post("{0}/oozie/v1/jobs".format(self.url), data=configuration, headers={'Content-Type': 'application/xml'})

        if response.status_code > 399:
            print response.headers["oozie-error-message"]
        print response.status_code
        print response.content
