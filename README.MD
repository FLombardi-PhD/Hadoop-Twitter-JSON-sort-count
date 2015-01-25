Jobs hadoop for processing a twitter dataset in json format.

- SortJson: sort the dataset per timestamp by inserting the complete json. This is necessary to recreate a real workload.
- SortJsonLite: sort the dataset per timestamp by inserting only the body of tweets (light datadriver version). 
- TSCount: count the num of tweet/ts. It is nececssary for learning the workload pattern.