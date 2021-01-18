import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import pickle
import time

from metaflow import FlowSpec, step, DoltDT, Parameter
import pandas as pd
from sklearn import tree

class MultiFlowDemo(FlowSpec):
    #bar_version = Parameter('bar-version',  help="Specifc the tag for the input version", required=True)
    @step
    def start(self):
        with DoltDT(run=self, database='foo', branch="master") as dolt:
            self.df = dolt.read_table('bar')

        time.sleep(1)
        self.next(self.middle)

    @step
    def middle(self):
        with DoltDT(run=self, database='foo', branch="master") as dolt:

            df = self.df
            df["B"] = df["B"].map(lambda x: x*2)

            dolt.write_table(table_name='baz', df=df, pks=['index'])

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    MultiFlowDemo()
