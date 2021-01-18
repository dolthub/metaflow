## User Story
A company maintains a shared Dolt database D that is used by multiple data scientists, all using Metaflow. The company realizes that D contains incorrect data and all models using D may be compromised. Is there a way for the company to identify all Metaflow flows and their owners that have relied on D over the past 30 days?

## Solution
Given the list of all flow names, a Dolt database, and a commit corresponding the corrupt data:
```python
for flow in flows:
    for run in flow:
        # Do stuff with DoltDT to indicate whether a flow read the problematic commit
```

## Questions
How is the namespace of flows across multiple users defined in this use-case? The algorithm is not complicated, but we need to understand more about how to locate the possibly "infected" flows.