from metaflow import FlowSpec, step, DoltDT
import json
import numpy as np


class DoltDemoFlow(FlowSpec):

    @step
    def start(self):
        import pandas
        self.df = pandas.read_csv('movies.csv')

        self.next(self.add_random)

    # Add a random number to the gross column.
    @step
    def add_random(self):
        import random

        with DoltDT(run=self, doltdb_path='metaflow_movies') as dolt:
            self.df['gross'] = self.df['gross'] + random.randint(1, 1000000)

            dolt.write_table(table_name='movies', df=self.df, pks=['movie_title'])

        self.next(self.end)

    @step
    def end(self):
        with DoltDT(run=self, doltdb_path='metaflow_demo') as dolt:
            dolt.commit_table_writes()


if __name__ == '__main__':
    DoltDemoFlow()
