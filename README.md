sentiment-analysis-trading
==========================

TODO
====

Reddit extraction :
-------------------

- Enhance ticker detection
    - [x] remove list type ??
    - [ ] Add name to ticker detection

Historical extraction :
-----------------------

-   [x] Load new files to BQ table
-   [ ] At the end of each day run the batch of the last day to complete BQ table

Daily extraction :
------------------
- [x] Upload each 10mn batch to GCS
- [ ] Add feature to extract for weekend & daily subs
- [ ] Add the data to postgres for daily alerting

New data sources :
------------------

- [ ] Add Bloomberg & other financial websites

R&D :
-----

- [ ] News article directly to GPT
- [ ] Prompt engineering for output as a json
- [ ] HOW TO stream comment to GPT for big subs ?
    - [ ] Add memory to remember which comment shave already been processed ?
    - [ ] Give full comments with their answers and ask model to give sentiment
      for each comment

Data eng:
---------

- [ ] Set up VPC
- [X] Set up GCS
- [X] Set up BQ
- [ ] Set up postgres


## Cheatsheet
### GCP
Cloud Function :
  - Deploy reddit extractor : `gcloud functions deploy reddit_extraction --runtime python39 --trigger-http --source data_extractor/reddit --region europe-west1`
  - Retrieve URL of service :  `URL=$(gcloud functions describe --region "europe-west1" reddit_extraction --format "value(httpsTrigger.url)")`
  - Authorize local call to cloud function: `curl -H "Authorization: bearer $(gcloud auth print-identity-token)" -w "\n" $URL`
  - Add arguments to call: `curl -H "Authorization: bearer $(gcloud auth print-identity-token)" -w "\n" $URL?subreddit=wallstreetbets&type=intraday`
