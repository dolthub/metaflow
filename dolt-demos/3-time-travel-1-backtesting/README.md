## User Story
Alice wants to train a model using daily data in D from January and February - this is the training set. She wants to validate the model using data from March - this is the test set. Questions:

1. How to construct the train and test split in Metaflow?
2. Alice runs many such experiments with different splits. She wants to analyze the results in a notebook after all experiments have completed. Can Alice see how the splits were constructed using the Metaflow Client API, given a Run object?

## Solution
This is straightforward to do in our current setup, as every write is associated with a commit. All Alice's reads and writes are uniquely reproducible, and so she can easily compare the splits used in different runs by retrieving the data associated with a a run.

## Questions
I think the only quesiton is whether we build any additional UX to solve for this use-case. I suspect not, but I could be convinced otherwise.