## User Story
Alice wants to train a model using the past 7 days of data in D. Bob informs Alice that the data from three days ago is invalid. Bob updates D to contain the correct data. How should the commit history of D and Alice’s code look like so she can ignore the old invalid data and only use the restated version?

## Solution
Each commit in a Dolt commit graph is "database" in the conventional sense. Bob needs only give Alice a commit, or just tell her to query the head of the branch he updated, to get the correct data.

## Questions
This seems sufficiently trivial that we might have missed something?