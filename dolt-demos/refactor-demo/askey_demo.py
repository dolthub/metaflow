import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

from metaflow import FlowSpec, step, DoltDT, Parameter, Run
from metaflow.datatools.dolt import DoltConfig
import pandas as pd

class AsKeyDemo(FlowSpec):
    @step
    def start(self):
        snapshot = Flow("VersioningDemo").latest_successful_run.data.dolt
        master_conf = DoltConfig(database="foo")
        with DoltDT(run=self, snapshot=snapshot) as dolt:
            df1 = dolt.read("bar", as_key="bar1")
        with DoltDT(run=self, config=master_conf) as dolt:
            df2 = dolt.read("bar", as_key="bar2")

        self.next(self.middle)

    @step
    def end(self):
        print(self.dolt)
        self.next(self.end)


if __name__ == '__main__':
    AsKeyDemo()
