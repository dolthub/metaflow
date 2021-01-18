## User Story
Alice has a flow F that reads data from a public Dolt database D. The database is updated occasionally. Alice wants to trigger an execution of F on Step Functions whenever new data is available. What’s the best way to do this?

## Solution

### DoltHub
We have webhook functionality, which although in beta could be pretty easily extended in an on-presmise deployment scenario.

### Dolt as a Service
For an instance of Dolt located on your own infrastructure, it would be easy enough to wire in some kind of polling mechanism that inspects the commit graph.

## Questions
I think this answer is highly dependent on what kind of deployment the user is operating on. Documentation could span Dolt, DoltHub, and Dolt SQL Server.