from requests import get, post, put
import os
from yattag import Doc
from json import loads
from pywebhdfs.webhdfs import PyWebHdfsClient
from datetime import datetime


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

    def set_endtime(self, thing, dt=datetime.now()):
        response = put("{0}/oozie/v1/job/{1}?action=change&value=endtime={2}".format(self.url, thing, dt.strftime("%Y-%m-%dT%H:%MZ")))
        if response.status_code != 200:
            raise RuntimeError("Failed talking to oozie. {}".format(response.content))
        return True

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

    def run(self, wf):
        doc, tag, text = Doc().tagtext()
        with tag("configuration"):
            with tag("property"):
                with tag("name"):
                    text("user.name")
                with tag("value"):
                    text("oozie")

            with tag("property"):
                with tag("name"):
                    text("oozie.wf.application.path")
                with tag("value"):
                    text("/"+wf.path + "/" + wf.name)

        configuration = doc.getvalue()
        response = post("{0}/oozie/v1/jobs?action=start".format(self.url), data=configuration, headers={'Content-Type': 'application/xml'})

        if response.status_code > 399:
            print response.headers["oozie-error-message"]
        print response.status_code
        print response.content
        return loads(response.content)

    def submit(self, bund, files=[]):
        hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')

        for f in files:
            hdfs.create_file("{}/{}".format(bund.path, f.name), f.read())  

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
                    text("/"+bund.path + "/" + bund.name)

        configuration = doc.getvalue()
        response = post("{0}/oozie/v1/jobs".format(self.url), data=configuration, headers={'Content-Type': 'application/xml'})

        if response.status_code > 399:
            print response.headers["oozie-error-message"]
        print response.status_code
        print response.content
