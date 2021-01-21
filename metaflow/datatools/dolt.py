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
    pathspec: str
    table_name: str
    config_id: str
    commit: str
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
class DoltRun(object):
    """
    Dolt lineage metadata used by the DoltDT to track data versions.
    Intended to be used as a metaflow artifact, JSON serializable.
    """
    default_config: str
    configs: List[DoltConfig]
    actions: List[DoltAction] = field(default_factory=list)

    @classmethod
    def from_json(self, data: str):
        return cls(**json.loads(data))


DoltRunT = Union[DoltRun, dict, str]
DoltConfigT = Union[DoltConfig, dict, str]


# TODO: expose other dolt functions?
#   - dolt config
#   - dolt log
#   - dolt creds
class DoltDT(object):

    def __init__(
        self,
        run: FlowSpec = None,
        run_snapshot FlowSpec = None,
        dolt_snapshot: DoltRunT = None,
        config: DoltConfigT = None,
    ):

        if run_snapshot and dolt_snapshot:
            raise ValueError(f"Specify only one of: `run_snapshot`, `dolt_snapshot`")
        elif run_snapshot and not hasattr(run_snapshot, 'dolt'):
            raise ValueError(f"run_snapshot must have .dolt attribute")
        elif run_snapshot and not isinstance(run_snapshot.dolt, DontRunT):
            raise ValueError(f"run_snapshot.dolt must be type `DoltRun`; found type(run_snapshot.dolt)")
        elif dolt_snapshot and not isinstance(dolt_snapshot, DontRunT):
            # TODO: handle dicts
            raise ValueError(f"dolt_snapshot must be type `DoltRun`; found type(run_snapshot.dolt)")
        elif run_snapshot
            self._read_snapshot = run_snapshot.dolt
        elif dolt_snapshot
            self._read_snapshot = dolt_snapshot

        if not instance(run, FlowSpec):
            raise ValueError(f"Arguement `run` must by type: metaflow.Run; found type(run)")

        self._run = run

        if not hasattr(self._run, 'dolt'):
            self._dolt = DoltRun()
            self._run.dolt = self._dolt
        elif not isinstance(self._run.dolt, DoltRun):
            # raise valueError?
            self._dolt = DoltRun()
            self._run.dolt = self._dolt
        else:
            self._dolt = self._run.dolt

        if config:
            self._config = config
            self._dolt.default_config = config
        else:
            self._config = self._dolt.default_config

        if isinstance(self._config, dict):
            self._config = DoltConfig(**self._config)

        # TODO: this doesn't consider remote/clones
        try:
            Dolt.init(repo_dir=self._config.database)
        except:
            pass

        self._doltdb = Dolt(repo_dir=self._config.database)

        current_branch, _ = self._doltdb.branch()
        self.entry_branch = None
        if current_branch.name != self._config.branch:
            self.entry_branch = current_branch
            self._doltdb.checkout(self._config.branch, checkout_branch=False)

        if DOLT_METAFLOW_ACTIONS not in self._doltdb.ls():
            # TODO: create metadata table?
            pass

    def __enter__(self):
        if not self._is_running:
            raise ValueError('Context manager use requires running flow')
        if not self._doltdb.status().is_clean:
            raise Exception('DoltDT as context manager requires clean working set for transaction semantics')

        self._new_actions = []
        self._start_run_attributes = set(vars(self._run).keys())
        return self

    def __exit__(self, *args, allow_empty: bool = True):
        # TODO: how to associate new variables with dolt actions?
        new_attributes = set(var(self._run).keys()) - self._start_run_attributes

        if not self._doltdb.status().is_clean:
            self.commit(message=self._auto_commit_message())

    def read(self, table, config: DoltConfigT = None):
        pass

    def write(self, table, config: DoltConfigT = None):
        pass

    def query(self, query: str, config: DoltConfigT = None):
        pass

    def tag(self):
        pass

    def commit(self, message: str):
        pass

    def _get_latest_commit_hash(self) -> str:
        lg = self._doltdb.log()
        return lg.popitem(last=False)[0]

    @classmethod
    def _auto_commit_message(cls):
        return f'{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}'


#@dataclass
#class DoltMeta:
    #flow_name: str
    #run_id: str
    #step_name: str
    #task_id: str
    #table_name: str
    #kind: str
    #database: str = "."
    #commit: str = None
    #timestamp: float = time.time()
    #data: pd.DataFrame = None

    #def dict(self):
        #return dict(
            #flow_name=self.flow_name,
            #run_id=self.run_id,
            #step_name=self.step_name,
            #task_id=self.task_id,
            #kind=self.kind,
            #database=self.database,
            #table_name=self.table_name,
            #commit=self.commit,
            #timestamp=self.timestamp,
        #)

    #def json(self):
        #return json.dumps(self.dict())


#class DoltRead(DoltMeta):
    #pass


#class DoltWrite(DoltMeta):
    #def set_commit(self, commit: str):
        #self.commit = commit


#class DoltRun(object):

    #def __init__(self, flow_name, run_id):
        #self._flow_name = flow_name
        #self._run_id = run_id
        #self._db_cache = {}
        #self._reads: Dict[str, Union[DoltRead, DoltWrite]] = {}
        #self._writes: Dict[str, Union[DoltRead, DoltWrite]] = {}
        #self._steps: List[str] = []
        ##self.metadb = Dolt(".")
        ##self.db_cache["."] = self.metadb

    #@property
    #def steps(self):
        ## use regular Client
        #pass

    #@property
    #def reads(self):
        #return self._get_actions_helper('read', DoltRead)

    #@property
    #def writes(self):
        #return self._get_actions_helper('write', DoltWrite)

    #def _get_actions_helper(self, action_str: str, action: type):
        #df = read_table_sql(self.metadb, _get_actions_query(self.flow_name, self.run_id, action_str))
        #databases = df.database.values
        #commits = df.commit.values
        #tables = df.table_name.values
        #dicts = df.to_dict('records')

        #res = []

        #row = 0
        #for db_name, commit, table_name in zip(databases, commits, tables):
            #db = self.db_cache.get('db_name', None) or Dolt(db_name)
            #table = read_table_sql(db, f'SELECT * FROM `{table_name}` AS OF "{commit}"')
            #read = action(data=table, **dicts[row])
            #res.append(read)
            #row += 1

        #return res


#def _get_actions_query(flow_name: str, run_id: str, action: str):
    #return f'''
        #SELECT
            #*
        #FROM
            #`metadata`
        #WHERE
            #flow_name = "{flow_name}"
            #AND run_id = "{run_id}"
            #AND kind = "{action}"
    #'''

#class DoltMeta(object):

    #def __init__(self):
        #pass

    #def reads():
        #pass

    #def writes():
        #pass


#class DoltDT(object):

    #def __init__(self, run = None, database: str = ".", branch: str = 'master'):
        #"""
        #Initialize a new context for Dolt operations with Metaflow.

        #run: this is either
            #- a FlowSpec when initialized with a running Flow
            #- a Flow when looking across for data read/written across runs of a Flow
            #- a Run when looking for data read/written by a specific run
        #doltdb_path: this is a path to a location on the filesystem with a Dolt database
        #"""
        #self.run = run
        #self.database = database
        #self.branch = branch
        #self.meta_database = "."

        #self.doltdb = Dolt(self.database)
        #try:
            #self.meta_doltdb = Dolt(os.getcwd())
        #except:
            #self.meta_doltdb = Dolt.init(os.getcwd())

        #current_branch, _ = self.doltdb.branch()
        #self.entry_branch = None
        #if current_branch.name != branch:
            #entry_branch = current_branch.name
            #self.doltdb.checkout(branch, checkout_branch=False)

        #self.table_reads = []
        #self.table_writes = []

    #def __enter__(self):
        #assert isinstance(self.run, FlowSpec) and current.is_running_flow, 'Context manager use requires running flow'
        #assert self.doltdb.status().is_clean, 'DoltDT as context manager requires clean working set for transaction semantics'
        #return self

    #def __exit__(self, *args, allow_empty: bool = True):
        #if not self.doltdb.status().is_clean:
            #self.commit_writes()
        #if self.table_reads or self.table_writes:
            #self.commit_metadata()

    #def _get_table_read(self, table: str) -> DoltRead:
        #return self._get_dolt_action('read', DoltRead, table)

    #def _get_table_write(self, table: str) -> DoltWrite:
        #return self._get_dolt_action('write', DoltWrite, table)

    #def _get_dolt_action(self, action_str: str, action: type, table: str):
        #return action(
            #flow_name=current.flow_name,
            #run_id=current.run_id,
            #step_name=current.step_name,
            #task_id=current.task_id,
            #commit=self._get_latest_commit_hash(),
            #table_name=table,
            #database=self.database,
            #kind=action_str,
        #)

    #def _get_latest_commit_hash(self) -> str:
        #lg = self.doltdb.log()
        #return lg.popitem(last=False)[0]

    #def write_metadata(self, data: List[DoltMeta]):
        #"""Important that write metadata commit is recorded immediately after the data commit"""
        ##meta_df = pd.DataFrame.from_records([x.dict() for x in self.table_reads + self.table_writes])
        ##import_df(repo=self.meta_doltdb, table_name="metadata", data=meta_df, primary_keys=meta_df.columns.tolist())
        

    #def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        #"""
        #Writes the contents of the given DataFrame to the specified table. If the table exists it is updated, if it
        #does not it is created.
        #"""
        #assert current.is_running_flow, 'Writes and commits are only supported in a running Flow'
        #import_df(repo=self.doltdb, table_name=table_name, data=df, primary_keys=pks)
        #self.table_writes.append(self._get_table_write(table_name))

    #def read_table(self, table_name: str, commit: str = None, flow_name: str = None, run_id: str = None) -> pd.DataFrame:
        #"""
        #Returns the specified tables as a DataFrame.
        #"""
        #if not current.is_running_flow:
            #raise ValueError("read_table is only supported in a running Flow")

        #read_meta = self._get_table_read(table_name)

        #if commit:
            #table = self._get_dolt_table_asof(self.doltdb, table_name, commit)
            #read_meta.commit = commit
        #elif flow_name and run_id:
            #df = read_table_sql(self.meta_doltdb, _get_actions_query(flow_name, run_id, 'read'))
            #database = df.database.values[0]
            #commit = df.commit.values[0]
            ## checkout database and get table ASOF commit
            #db = Dolt(database)
            #table = self._get_dolt_table_asof(db, table_name, commit)
            #read_meta.commit = commit
        #else:
            #table = read_table(self.doltdb, table_name)
            #read_meta.commit = self._get_latest_commit_hash()
        #self.table_reads.append(read_meta)
        #return table

    #def commit_writes(self, allow_empty=True):
        #"""
        #Creates a new commit containing all the changes recorded in self.dolt_data.['table_writes'], meaning that the
        #precise data can be reproduced exactly later on by querying self.flow_spec.
        #"""
        #if not current.is_running_flow:
            #raise ValueError('Writes and commits are only supported in a running Flow')

        #to_commit = [table_write.table_name for table_write in self.table_writes + self.table_reads]
        #self.doltdb.add(to_commit)
        #self.doltdb.commit(message=self._get_commit_message(), allow_empty=allow_empty)

    #def commit_metadata(self, allow_empty=True):
        #commit_hash = self._get_latest_commit_hash() # might be different db
        #for w in self.table_writes:
            #w.set_commit(commit_hash)

        #self.write_metadata(self.table_reads + self.table_writes)
        #self.meta_doltdb.add("metadata")
        #return self.meta_doltdb.commit(message=self._get_commit_message(), allow_empty=allow_empty)

    #@classmethod
    #def _get_commit_message(cls):
        #return f'{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}'

    #@classmethod
    #def _get_dolt_table_asof(cls, dolt: Dolt, table_name: str, commit: str = None) -> pd.DataFrame:
        #base_query = f'SELECT * FROM `{table_name}`'
        #if commit:
            #return read_table_sql(dolt, f'{base_query} AS OF "{commit}"')
        #else:
            #return read_table_sql(dolt, base_query)
