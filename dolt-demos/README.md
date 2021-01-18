# Dolt in Metaflow

## Background
Dolt is a SQL database with Git-like version control semantics for both data and schema. This folder contains demo code showing how to use the Dolt datatool inside Metaflow to get the benefits of Dolt's version control features.

## Goals
Assuming we have an instance of Dolt, in which we are storing some input data, and perhaps writing intermediate results to, we would like to do the following:
- given a Flow definition, stage, and run, succinctly retrieve the exact Dolt data used as input
- given a set of Flows, and one or more runs associated with each, easily verify they used the same input data
- given a Flow, a and a set of associated runs, succinctly obtain the data output to Dolt by each run

## Demo:

1. Reproducibility
    1. [dolthub input to flow](./1-reproducibility-1-dolthub)
    2. [resume flow](./1-reproducibility-2-resume)
    3. [database versioning](./1-reproducibility-3-versioning)

2. Lineage
    1. [linear]() (skipped)
    2. [multiple flows](./2-lineage-2-multiple-flows)

## Implementation
The `metaflow.datatools.DoltDT` class is implemented to effectively serve two
use cases: acting as a context manager for reading and writing from Dolt
in a running flow, and querying the data read and written by a run of a flow.

### Running Metaflow
The core of the `DoltDT` functionality when acting as a context manager 
for a running flow is supporting read, write, and commit operations
against Dolt while recording metadata about those interactions in 
a Dolt database:

```
> dolt sql -q "SELECT * from metadata;"
+---------------+------------------+-----------+---------+-------+----------+------------+------------+---------------+
| flow_name     | run_id           | step_name | task_id | kind  | database | table_name | commit     | timestamp     |
+---------------+------------------+-----------+---------+-------+----------+------------+------------+---------------+
| MultiFlowDemo | 1610863084944857 | middle    | 2       | write | foo      | baz        | al0ehd852d | 1.6108631e+09 |
| MultiFlowDemo | 1610863084944857 | start     | 1       | read  | foo      | bar        | al0ehd852d | 1.6108631e+09 |
| MultiFlowDemo | 1610863102018770 | middle    | 2       | write | foo      | baz        | al0ehd852d | 1.6108631e+09 |
| MultiFlowDemo | 1610863102018770 | start     | 1       | read  | foo      | bar        | al0ehd852d | 1.6108631e+09 |
+---------------+------------------+-----------+---------+-------+----------+------------+------------+---------------+
```

The operations currently supported are reading, writing,
and committing Pandas `DataFrame` objects:
```python3
class DoltDT:

    def read_table(self, table_name: str) -> pd.DataFrame:
        pass

    def write_table(self, table_name: str, df: pd.DataFrame, pks: List[str]):
        pass

    def commit_writes(self, allow_empty=True):
        pass
```

Each of these methods uses the objects defined above to snapshot the relevant
metadata when the read is made, or update that metadata with the commit hash
if the operation is commit. Because these metadata snapshots tie into the
Metaflow object hierachy in a natural way, they expose query patterns that
are intuitive to users familiar with that object hierarchy. 

In the next section we look at the UX for querying Dolt data via the Metaflow object hierarhcy.

### Querying Metaflow Runs
We ended the last section by pointing out that the read, write, and commit
tracking inside the the Metaflow metadata tracking system led to a unique
degree of reproducibility. `DoltDT` provides some tools for querying Dolt
by using the stored metadata return data in a manner that ties neatly into
the Metaflow object hierarchy.

```python3
class DoltRun:

    def __init__(self, flow_name, run_id):
        pass

    def reads(self):
        pass

    def writes(self):
        pass
```

The anatomy of a query is as follows:

1. Select the rows of interest from the Dolt metadata table ->
   `List[DoltRead]` or `List[DoltWrite]`

2. For each read/write, load the table as a dataframe using a
    `SELECT * from {database}.{table} ASOF {commit}` query.

3. Return the full `List[DoltRead]` / `List[DoltWrite]` with dataframes
   accessible as the `.data` attribute.

These commands work similary using the Dolt CLI, and arbitrary customary
queries can be implemented with the same SQL syntax.

## Collaboration
WIP

## Questions and Open Items

### New Questions

1. Version tracking -- explicit input parameters, implicit read/write capture, or both?
2. How is metadata resolved locally vs. remote? (Ex: two people sharing flow metadata)
3. Preferred data access interface -- client API? doltpy? datatool?
4. Other dolt features
    1. Tags
    2. Diffs
    3. Merges

### Questions
1. How could improve the UX to more neatly match the use-cases that currently exist in Netflix and the wider Metaflow community?
2. Does the way we have architected the `DoltDT` class make sense, and in particular is it the right approach to have that single class serve running flows and post-run querying of different elements of the flow object hierarchy?
3. Currently this is implemented assuming that the Dolt database being used exists on the filesystem of the machine/container that launches the flow, this might not be viable, and there are number of ways this could be handled

### To Do
1. For this to be robust and usable in a production setting there needs to be a lot more tests
2. There needs to be clear and detailed guidance on an appropriate workflow for collaboration with Dolt and Metaflow
