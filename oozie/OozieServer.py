from requests import get, post
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

    def submit(self, job):
        content = job.as_xml()
        response = post("{0}/oozie/v1/jobs".format(self.url), data=content, headers={'Content-Type': 'application/xml'})
        print response.status_code
        print response.content

