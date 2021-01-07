from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from .. import FlowSpec
from ..current import current
from typing import List, Mapping, Union
import pandas as pd


class DoltTableRead:
    def __init__(self, run_id: int, step: str, branch: str, commit: str, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = commit
        self.branch = branch
        self.table_name = table_name


class DoltTableWrite:
    def __init__(self, run_id: int, step: str, table_name: str):
        self.run_id = run_id
        self.step = step
        self.commit = None
        self.branch = None
        self.table_name = table_name

    def set_commit_and_branch(self, commit: str, branch: str):
        self.commit = commit
        self.branch = branch


class DoltDT(object):

    def __init__(self, run, doltdb_path: str, branch: str = 'master'):
        """
        Initialize a new context for Dolt operations with Metaflow.

        run: this is either
            - a FlowSpec when initialized with a running Flow
            - a Flow when looking across for data read/written across runs of a Flow
            - a Run when looking for data read/written by a specific run
        doltdb_path: this is a path to a location on the filesystem with a Dolt database
        """
        self.doltdb = Dolt(doltdb_path)
        self.run = run
        self.branch = branch

        if not hasattr(self.run, 'dolt') and isinstance(self.run, FlowSpec):
            self.run.dolt = {}
            self.dolt_data = self.run.dolt
            self.dolt_data['table_reads'] = []
            self.dolt_data['table_writes'] = []
        elif isinstance(self.run, FlowSpec):
            self.dolt_data = self.run.dolt
        else:
            self.dolt_data = self.run.data.dolt

        current_branch, _ = self.doltdb.branch()
        self.entry_branch = None
        if current_branch.name != self.branch:
            self.entry_branch = current_branch
            self.doltdb.checkout(branch, checkout_branch=False)

    def __enter__(self):
        assert isinstance(self.run, FlowSpec) and current.is_running_flow, 'Context manager use requires running flow'
        return self

    def __exit__(self, *args):
        self.commit_table_writes()

    def _get_table_read(self, table: str) -> DoltTableRead:
        return DoltTableRead(current.run_id, current.step_name, self.branch, self._get_latest_commit_hash(), table)

    def _get_table_write(self, table: str) -> DoltTableWrite:
        return DoltTableWrite(current.run_id, current.step_name, table)

    def _get_latest_commit_hash(self) -> str:
        lg = self.doltdb.log()
        return lg.popitem(last=False)[0]

    def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        """
        Writes the contents of the given DataFrame to the specified table. If the table exists it is updated, if it
        does not it is created.
        """
        assert current.is_running_flow, 'Writes and commits are only supported in a running Flow'
        import_df(repo=self.doltdb, table_name=table_name, data=df, primary_keys=pks)
        self.doltdb.add(table_name)
        self.dolt_data['table_writes'].append(self._get_table_write(table_name))

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Returns the specified tables as a DataFrame.
        """
        assert current.is_running_flow, 'read_table is only supported in a running Flow'
        table = read_table(self.doltdb, table_name)
        self.dolt_data['table_reads'].append(self._get_table_read(table_name))
        return table

    def commit_table_writes(self, allow_empty=True):
        """
        Creates a new commit containing all the changes recorded in self.dolt_data.['table_writes'], meaning that the
        precise data can be reproduced exactly later on by querying self.flow_spec.
        """
        assert current.is_running_flow, 'Writes and commits are only supported in a running Flow'
        to_commit = [table_write.table_name for table_write in self.dolt_data['table_writes']]
        self.doltdb.add(to_commit)
        self.doltdb.commit(message='Run {}'.format(current.run_id), allow_empty=allow_empty)
        commit_hash = self._get_latest_commit_hash()
        current_branch, _ = self.doltdb.branch()
        # TODO
        #   are we sure that we are only going to get the table_writes associated with the specific flow spec running
        #   here?
        for table_write in self.dolt_data['table_writes']:
            if not table_write.commit:
                table_write.set_commit_and_branch(current_branch.name, commit_hash)

    def get_reads(self, runs: List[int] = None, steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
        """
        Returns a nested map of the form:
            {run_id/step: [{table_name: pd.DataFrame}]}

        That is, for a Flow or Run, a mapping from the run_id, and step, to a list of table names and table data read
        by the step associated identified by the key.
        """
        assert not current.is_running_flow, 'Getting reads not supported in a running Flow'
        table_reads = self._get_table_access_record_helper('table_reads', steps)
        return self._get_tables_for_access_records(table_reads, runs, steps)

    def get_writes(self, runs: List[int] = None, steps: List[str] = None) -> Mapping[str, Mapping[str, pd.DataFrame]]:
        """
        Returns a nested map of the form:
            {run_id/step: [{table_name: pd.DataFrame}]}

        That is, for a Flow or Run, a mapping from the run_id, and step, to a list of table names and table data written
        by the step associated identified by the key.
        """
        assert not current.is_running_flow, 'Getting reads not supported in a running Flow'
        table_writes = self._get_table_access_record_helper('table_writes', steps)
        return self._get_tables_for_access_records(table_writes, runs, steps)

    def _get_table_access_record_helper(self, access_record_key: str, steps: list):
        access_records = []
        if isinstance(self.run, FlowSpec):
            runs = [run for run in self.run]
        else:
            runs = [self.run]

        for run in runs:
            for step in run:
                if step.id in steps:
                    access_records.extend(step.task.data.dolt[access_record_key])

        return access_records

    def _get_tables_for_access_records(self,
                                       access_records: Union[List[DoltTableRead], List[DoltTableWrite]],
                                       runs: List[int],
                                       steps: List[str]) -> Mapping[str, Mapping[str, pd.DataFrame]]:
        result = {}
        for access_record in access_records:
            if runs and access_record.run_id not in runs and steps and not access_record.step not in steps:
                pass
            else:
                run_step_path = '{}/{}'.format(access_record.run_id, access_record.step)
                df = self._get_dolt_table_asof(access_record.table_name, access_record.commit)
                if run_step_path in result:
                    result[run_step_path][access_record.table_name] = df
                else:
                    result[run_step_path] = {access_record.table_name: df}

        return result

    def _get_dolt_table_asof(self, table_name: str, commit: str) -> pd.DataFrame:
        return read_table_sql(self.doltdb, 'SELECT * FROM {} AS OF "{}"'.format(table_name, commit))
