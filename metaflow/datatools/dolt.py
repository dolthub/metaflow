from collections import defaultdict
from dataclasses import dataclass, field
import json
import os
import time
from typing import Dict, List, Union
import uuid

import pandas as pd

from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from .. import FlowSpec
from ..client import Run
from ..current import current

DOLT_METAFLOW_ACTIONS = "metaflow_actions"

@dataclass
class DoltAction:
    """
    Describes an interaction with a Dolt database within a
    DoltDT context manager.
    """
    key: str
    config_id: str

    pathspec: str
    table_name: str
    commit: str = None
    kind: str = "read"
    query: str = None
    artifact_name: str = None
    timestamp: float = field(default_factory=lambda: time.time())

    def dict(self):
        return dict(
            pathspec=self.pathspec,
            table_name=self.table_name,
            config_id=self.config_id,
            commit=self.commit,
            query=self.query,
            kind=self.kind,
            artifact_name=self.artifact_name,
            timestamp=self.timestamp,
        )

@dataclass
class DoltConfig:
    """
    Configuration for connecting to a Dolt database.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    database: str = "."
    branch: str = "master"
    commit: str = None
    dolthub_remote: bool = False
    push_on_commit: bool = False
    #fully_qualified_name: str

    def dict(self):
        return dict(
            id=self.id,
            database=self.database,
            branch=self.branch,
            commit=self.commit,
            dolthub_remote=self.dolthub_remote,
            push_on_commit=self.push_on_commit,
        )

@dataclass
class DoltSnapshot(object):
    """
    Dolt lineage metadata used by the DoltDT to track data versions.
    Intended to be used as a metaflow artifact, JSON serializable via .dict().
    """
    actions: Dict[str, DoltAction] = field(default_factory=dict)
    configs: List[DoltConfig] = field(default_factory=list)

    def dict(self):
        return dict(
            actions=self.actions,
            configs=self.configs,
        )

    @classmethod
    def from_json(self, data: str):
        return cls(**json.loads(data))


#DoltRunT = Union[DoltRun, dict, str]
#DoltConfigT = Union[DoltConfig, dict, str]


# TODO: expose other dolt functions?
#   - dolt config
#   - dolt log
#   - dolt creds

def runtime_only(f):
    @wraps
    def inner(*args, **kwargs):
        if current.is_running:
            return
        return f(*args, **kwargs)

def snapshot_unsafe(f):
    @wraps
    def inner(*args, **kwargs):
        if current.is_running:
            return
        return f(*args, **kwargs)

class DoltDTBase(object):

    def __init__(self, run: FlowSpec, snapshot: Optional[dict], config: Optional[DoltConfig] = None):

        if not hasattr(self._run, 'dolt'):
            self._run.dolt = DoltSnapshot().dict()
        elif not isinstance(self._run.dolt, dict):
            self._run.dolt = DoltSnapshot().dict()

        self._dolt = self._run.dolt
        self._run = run
        self._dbcache = {} # configid -> Dolt instance
        self._new_actions = {} # keep track of write state to commit at end

    def __enter__(self):
        if not self._is_running:
            raise ValueError('Context manager use requires running flow')
        if not self._doltdb.status().is_clean:
            raise Exception('DoltDT as context manager requires clean working set for transaction semantics')

        self._start_run_attributes = set(vars(self._run).keys())
        return self

    def __exit__(self, *args, allow_empty: bool = True):
        # TODO: how to associate new variables with dolt actions?
        new_attributes = set(var(self._run).keys()) - self._start_run_attributes

        #if not self._doltdb.status().is_clean:
            #self.commit(message=self._pathspec())

    def read(self, tablename: str):
        raise NotImplementedError()

    @snapshot_unsafe
    def query(self, query_string: str):
        pass

    @runtime_only
    @snapshot_unsafe
    def write(self, tablename: str):
        pass

    def _execute_read_action(self, action: DoltAction, config: DoltConfig):
        # get a table
        pass

    @runtime_only
    @snapshot_unsafe
    def _execute_write_action(self, action: DoltAction):
        # record a table write if not running
        pass

    @runtime_only
    def _add_action(self, action: DoltAction):
        # pass if not running
        # otherwise add to self._new_actions
        # also add to run.dolt

    def _commit_actions(self):
         # find writes in new actions
         # add those tables
         # commit them
         # update the action references with the new commit

    def _get_db(self, config):
        # reference the dbcache by configid, or load the config and save
        #try:
            #Dolt.init(repo_dir=self._config.database)
        #except:
            #pass

        #self._doltdb = Dolt(repo_dir=self._config.database)

        #current_branch, _ = self._doltdb.branch()
        #self.entry_branch = None
        #if current_branch.name != self._config.branch:
            #self.entry_branch = current_branch
            #self._doltdb.checkout(self._config.branch, checkout_branch=False)
        pass

    def _get_latest_commit_hash(self) -> str:
        lg = self._doltdb.log()
        return lg.popitem(last=False)[0]

    @staticmethod
    def _pathspec(cls):
        return f'{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}'

    @staticmethod
    def _get_table_asof(cls, dolt: Dolt, table_name: str, commit: str = None) -> pd.DataFrame:
        base_query = f'SELECT * FROM `{table_name}`'
        if commit:
            return read_table_sql(dolt, f'{base_query} AS OF "{commit}"')
        else:
            return read_table_sql(dolt, base_query)


class DoltSnapshotDT(DoltDTBase):

    def __init__(self, run: FlowSpec, snapshot: dict):
        """
        Can only read from a SnapshotDT, and reading is isolated to the snapshot.
        """
        super().__init__(run)
        self._read_snapshot = snapshot
        self._sactions = snapshot["actions"]
        self._sconfigs = snapshot["configs"]

    def read(self, key, as_key: Optional[str] = None):
        # reads limited to  snapshot keys
        snapshot_action = self._sactions.get(key, None)
        if not snapshot_action:
            raise ValueError("Key not found in snapshot")
        action = snapshot_action.copy()
        action["key"] = as_key
        action["kind"] = "read"

        table = self._execute_read_action(action, config)
        self._add_action(action)
        return table

class DoltCommitDT(DoltDTBase)

    def __init__(self, run: FlowSpec, config: DoltConfig):
        """
        Can read or write with Dolt, starting from a single reference commit.
        """
        self._config = config

    def read(self, key: str, as_key: Optional[str] = None):
        # reads limited to config
        action = DoltAction(
            kind="read",
            key=as_key,
            **self._config,
        )
        table = self._execute_read_action(action, config)
        self._add_action(action)
        return table

def DoltDT(run, snapshot, config):
    if config and snapshot:
        raise ValueError("Specify snapshot or config mode, not both.")
    elif snapshot:
        return DoltSnapshotDT(run, snapshot)
    elif config:
        return DoltCommitDT(run, config)
    else:
        raise ValueError("Specify one of: snapshot, config")
