import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import pickle
import time

from metaflow import FlowSpec, step, DoltDT, Parameter
from metaflow.datatools.dolt import DoltRun
import pandas as pd
from sklearn import tree

class MultiFlowDemo2(FlowSpec):
    flow_dep = Parameter('flow-dep',  help="Specifc the tag for the input version", required=True)
    @step
    def start(self):
        flow, run = self.flow_dep.split("/")
        d = DoltRun(flow_name=flow, run_id=run)
        f_input = d.reads[0]
        f_output = d.writes[0]
        with DoltDT(run=self) as dolt:
            self.inp1 = dolt.read_table(f_input.table_name, commit=f_input.commit)
            self.inp2 = dolt.read_table(f_output.table_name, commit=f_output.commit)

        self.next(self.middle)

    @step
    def middle(self):
        with DoltDT(run=self) as dolt:

            df = self.inp1 + self.inp2

            dolt.write_table(table_name='baz', df=df, pks=['index'])

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    MultiFlowDemo2()
