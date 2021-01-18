## User Story
Alice has a Dolt table X and a flow myflow.py that reads from X. Imagine the following sequence of actions:
1. Alice runs python `myflow.py` run, resulting in Run ID 1.
2. Alice updates X, possibly resulting to X’. She doesn’t know if the upstream has changed.
3. Alice wants to understand if she needs to re-run the flow to match the latest version of the table.
4. Alice opens a notebook to see if the data used by Run ID 1 corresponds to the current state of the table, X’, i.e. has the data updated since the Run 1?

## Solution
To solve for this Alice can retrieve all the reads associated with flow with run ID 1. She locates the appropriate read records that correspond to the table in question. She then executes a `dolt diff` between the current state of the Dolt database, and her the commit her flow read at, for the table in question.

In practice we will implement a method that checks this with a single function call. In particular it will be something like:
```python
def run_is_current(pathspec: str) -> Mapping[str, bool]:
    pass
```

This will return a mapping that maps all input tables to whether the version used at the time is current.