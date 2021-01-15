from dataclasses import dataclass
import glob
import json
import os
import random
from typing import List, Mapping, Union

import pandas as pd

from doltpy.core import Dolt, DoltException
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from .. import FlowSpec
from ..client import Run
from ..current import current


@dataclass
class DoltMeta:
    run_id: str
    step_name: str
    task_id: str
    doltdb_path: str
    table_name: str
    commit: str = None
    branch: str = None

    def dict(self):
        return dict(
            run_id=self.run_id,
            step_name=self.step_name,
            task_id=self.task_id,
            doltdb_path=self.doltdb_path,
            table_name=self.table_name,
            branch=self.branch,
            commit=self.commit,
        )

    def json(self):
        return json.dumps(self.dict())


class DoltTableRead(DoltMeta):
    pass


@dataclass
class DoltTableWrite(DoltMeta):
    def set_commit_and_branch(self, commit: str, branch: str):
        self.commit = commit
        self.branch = branch

class DoltClient(object):

    def __init__(self, pathspec):
        parts = pathspec.split("/")
        if len(parts) < 2:
            raise Exception("Partial pathspec required")
        self.flow_name = parts[0]
        self.run_id = parts[1]

        self._dir = os.path.join(".metaflow", ".dolt", self.flow_name, self.run_id)
        self.steps = os.listdir(self._dir)
        self.databases_cache = {}


    def step_artifacts(self, step_name):
        run = Run(f"{self.flow_name}/{self.run_id}")
        res = {}
        meta = [DoltMeta(**json.load(open(p))) for p in glob.glob(os.path.join(self._dir, step_name, "**", "*"))]
        for m in meta:
            key = (m.doltdb_path, m.branch or m.commit)
            if key not in self.databases_cache:
                self.databases_cache[key] = DoltDT(run, doltdb_path=key[0], branch=key[1])
            doltdt = self.databases_cache[key]
            data = doltdt.get_writes(steps=[step_name])
            for step, tables in data.items():
                key = list(tables.keys())[0]
                res[key] = data[step][key]
        return res


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
        self.doltdb_path = doltdb_path
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
            try:
                self.doltdb.checkout(branch, checkout_branch=False)
            except DoltException as e:
                self.doltdb.checkout(branch, checkout_branch=True, start_point=f"tmp_{random.randrange(1e20)}")

    def __enter__(self):
        assert isinstance(self.run, FlowSpec) and current.is_running_flow, 'Context manager use requires running flow'
        assert self.doltdb.status().is_clean, 'DoltDT as context manager requires clean working set for transaction semantics'
        return self

    def __exit__(self, *args):
        current_writes = [table_write for table_write in self.dolt_data['table_writes']
                          if table_write.run_id == current.run_id
                          and table_write.step_name == current.step_name
                          and table_write.task_id == current.task_id
                          and table_write.commit is None]
        if current_writes:
            self.commit_table_writes()

        for write in current_writes:
            write.commit=self._get_latest_commit_hash()
            self._record_metadata(write)

    def _get_table_read(self, table: str) -> DoltTableRead:
        return DoltTableRead(
            run_id=current.run_id,
            step_name=current.step_name,
            task_id=current.task_id,
            table_name=table,
            doltdb_path=self.doltdb_path,
            branch=self.branch,
            commit=self._get_latest_commit_hash(),
        )

    def _get_table_write(self, table: str) -> DoltTableWrite:
        return DoltTableWrite(
            run_id=current.run_id,
            step_name=current.step_name,
            task_id=current.task_id,
            table_name=table,
            doltdb_path=self.doltdb_path,
            branch=self.branch,
        )

    def _get_latest_commit_hash(self) -> str:
        lg = self.doltdb.log()
        return lg.popitem(last=False)[0]

    def _record_metadata(self, write: DoltMeta):
        write_path = os.path.join(".metaflow", ".dolt", self.run.name, write.run_id, write.step_name, write.task_id, write.table_name)
        if not os.path.exists(os.path.dirname(write_path)):
            os.makedirs(os.path.dirname(write_path))
        with open(os.path.join(".metaflow", ".dolt", self.run.name, write.run_id, write.step_name, write.task_id, write.table_name), "w") as f:
            f.write(write.json())
        return

    def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        """
        Writes the contents of the given DataFrame to the specified table. If the table exists it is updated, if it
        does not it is created.
        """
        assert current.is_running_flow, 'Writes and commits are only supported in a running Flow'
        import_df(repo=self.doltdb, table_name=table_name, data=df, primary_keys=pks)
        self.doltdb.add(table_name)
        write = self._get_table_write(table_name)
        self.dolt_data['table_writes'].append(write)

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
        self.doltdb.commit(message=self._get_commit_message(), allow_empty=allow_empty)
        commit_hash = self._get_latest_commit_hash()
        current_branch, _ = self.doltdb.branch()
        # TODO
        #   are we sure that we are only going to get the table_writes associated with the specific flow spec running
        #   here?
        for table_write in self.dolt_data['table_writes']:
            if not table_write.commit:
                table_write.set_commit_and_branch(current_branch.name, commit_hash)

    @classmethod
    def _get_commit_message(cls):
        return '{flow_name}/{run_id}/{step_name}/{task_id}'.format(flow_name=current.flow_name,
                                                                   run_id=current.run_id,
                                                                   step_name=current.step_name,
                                                                   task_id=current.task_id)

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
            # This filters out runs and steps that are not in the runs and steps specified as function parameters
            if runs and access_record.run_id not in runs and steps and not access_record.step_name not in steps:
                pass
            else:
                run_step_path = '{}/{}'.format(access_record.run_id, access_record.step_name)
                df = self._get_dolt_table_asof(access_record.table_name, access_record.commit)
                if run_step_path in result:
                    result[run_step_path][access_record.table_name] = df
                else:
                    result[run_step_path] = {access_record.table_name: df}

        return result

    def _get_dolt_table_asof(self, table_name: str, commit: str) -> pd.DataFrame:
        return read_table_sql(self.doltdb, 'SELECT * FROM `{}` AS OF "{}"'.format(table_name, commit))

    @staticmethod
    def get_task_for_table(table_name: str, commit: str):
        """
        Given a table name, traverse the Dolt commit graph and retrieve the specified commit, then use the stored
        task path to try and retrieve the task object from Metaflow.
        """
        pass

    @staticmethod
    def get_tasks_for_table(table_name: str):
        """
        Given a table name, locate the list of commits that contain changes to the table, then use the task paths
        stored in the corresponding messages to retrieve the task instances from Metaflow and return them.
        """
        pass
