
## User Story
Alice has a Dolt database D and a flow myflow.py that reads data from D. Imagine the following sequence of actions:

1. Alice runs python myflow.py run, resulting in Run ID 1.
2. A week later, Bob wants to work with Alice. Using the client API, he finds that the latest successful run is 1. He is able to pull the corresponding code to his laptop using the Metaflow Client API.
3. Bob wants to start by reproducing Alice’s exact results from Run ID 1. He has the exact version of Alice’s code but not her data D. What’s the easiest way for Bob to obtain a version of the database that Alice used and confirm that it contains the correct data?

## Solution
We can tool the integration in such a way that users can run a flow "as of" another run ID. So, in this case Bob would run the specified flow "as of" run ID 1, which would cause the integration to read the precise data that run ID 1 read, using the metadata store to retrieve the commit required to query the relevant data. Users will need to expose that as a parameter to their flow.