import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import pickle

from metaflow import FlowSpec, step, DoltDT, Parameter, Run
from metaflow.datatools.dolt import DoltConfig
import pandas as pd
from sklearn import tree

class SnapshotDemo(FlowSpec):
    read_run_id = Parameter('read-run-id',  help="Pass a run_id for a VersionDemo flow", required=True)
    @step
    def start(self):
        snapshot = Run(f"VersioningDemo/{self.read_run_id}").data.dolt
        with DoltDT(run=self, snapshot=snapshot) as dolt:
            df = dolt.read('bar')

        self.next(self.middle)

    @step
    def end(self):
        print(self.dolt)
        self.next(self.end)


if __name__ == '__main__':
    SnapshotDemo()
