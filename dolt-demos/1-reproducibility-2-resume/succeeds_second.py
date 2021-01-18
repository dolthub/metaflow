import datetime
import pickle
import pytz

from metaflow import FlowSpec, step, DoltDT, Parameter, Flow
import pandas as pd
from sklearn import tree

class SucceedsSecondDemo(FlowSpec):

    bar_version = Parameter('bar-version',  help="Specifc the tag for the input version", required=True)

    @step
    def start(self):
        with DoltDT(run=self, database='foo', branch="master") as dolt:
            self.df = dolt.read_table('bar')

        first_run = Flow("SucceedsFirstDemo").latest_successful_run
        first_run_ts = datetime.datetime.strptime(first_run.finished_at, "%Y-%m-%dT%H:%M:%SZ")
        one_minute_ago = datetime.datetime.now() + datetime.timedelta(hours=8) - datetime.timedelta(minutes=1)
        if first_run_ts < one_minute_ago:
            raise Exception("Run `FirstDemo` within one minute of `SecondDemo`")

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
    SucceedsSecondDemo()
