from requests import get, post, put
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

    def bundles(self, status):
        response = get("{0}/oozie/v1/jobs?jobtype=bundle".format(self.url))
        if response.status_code != 200:
            raise RuntimeError("Couldn't reach oozie server. Is the provided url correct?")
        else:
            return [job for job in loads(response.content)['bundlejobs'] if job['status'] == status]

    def set_status(self, thing, status):
        response = put("{0}/oozie/v1/job/{1}?action={2}".format(self.url, thing, status))
        if response.status_code != 200:
            raise RuntimeError("Couldn't reach oozie server. Is the provided url correct?")

        return True

    def version(self):
        response = get("{0}/oozie/v1/admin/build-version".format(self.url))
        if response.status_code != 200:
            raise RuntimeError("Couldn't reach oozie server. Is the provided url correct?")
        else:
            return loads(response.content)["buildVersion"]

    def submit(self, bundle_name, coords, files=[]):
        hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')
        deployment_path = "user/oozie/bundles/{0}".format(bundle_name)
        bundle_path = "{0}/bundle.xml".format(deployment_path, bundle_name)
        bund = bundle.Bundle(bundle_name)

        for coordinator in coords:
            bund.add(coordinator)
        
        hdfs.create_file(bundle_path, bund.as_xml())

        for f in files:
            hdfs.create_file("{}/{}/{}".format(deployment_path, bundle_name, f.name), f.read())  

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
