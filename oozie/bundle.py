from yattag import Doc, indent
import os
from pywebhdfs.webhdfs import PyWebHdfsClient


class Bundle:
    def __init__(self, name):
        self.name = name
        self.coordinators = ()

    def add(self, coordinator):
        self.coordinators = self.coordinators + (coordinator,)

    def save(self):
        hdfs = PyWebHdfsClient(host=os.environ["WEBHDFS_HOST"], port='14000', user_name='oozie')
        deployment_path = "user/oozie/bundles/{0}".format(self.name)
        bundle_path = "{0}/bundle.xml".format(deployment_path, self.name)
        
        hdfs.create_file(bundle_path, self.as_xml())

    def as_xml(self):
        doc, tag, text = Doc().tagtext()
        doc.asis("<?xml version='1.0' encoding='UTF-8'?>")
        with tag('bundle-app', name=self.name, xmlns="uri:oozie:bundle:0.1"):
            for coordinator in self.coordinators:
                with tag("coordinator", name=coordinator.name):
                    with tag("app-path"):
                        text("/"+coordinator.path + "/" + coordinator.name)

        return indent(doc.getvalue())

