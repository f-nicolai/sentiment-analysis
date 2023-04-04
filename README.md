sentiment-analysis-trading
==========================

TODO
====

Reddit extraction :
-------------------
- GLOBAL
  - [x] Stream new comments & submissions continuously via  `reddit.stream.comments`
  - [x] Deploy above code on VM to run 24/7
  - [x] upload each comment & submisison on a separated file on GCS 
    - [ ] Later replace GCS with Postgres
  - [ ] Add retries to streaming
  - [ ] parametrize historical extraction to handle 2 in // just with arguments
  - [ ] Dockerize everything on VM
  - [ ] Modify `last_submissions_data` cloud function to only update post's up votes & meta data (should be much faster => every 5 mn ?
  - [ ] Dockerize code on VM, and add auto reboot if job fails
  - [ ] Package code properly to remove the `sys.path.append()` que je ne saurais voir


- Enhance ticker detection
    - [ ] Add name to ticker detection

Historical extraction :
-----------------------

-   [x] Load new files to BQ table
-   [ ] At the end of each day run the batch of the last day to complete BQ table

Daily extraction :
------------------
- [x] Upload each 10mn batch to GCS
- ~~[ ] Add feature to extract for weekend & daily subs~~
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

- [X] Set up GCS
- [X] Set up BQ
- [X] Deploy VM
- [ ] Set up postgres
- ~~[ ] Set up VPC~~ (Later maybe)


## Cheatsheet
### GCP
Cloud Function :
  - Deploy reddit extractor : `gcloud functions deploy reddit_extraction --runtime python39 --trigger-http --source data_extractor/reddit --region europe-west1`
  - Retrieve URL of service :  `URL=$(gcloud functions describe --region "europe-west1" reddit_extraction --format "value(httpsTrigger.url)")`
  - Authorize local call to cloud function: `curl -H "Authorization: bearer $(gcloud auth print-identity-token)" -w "\n" $URL`
  - Add arguments to call: `curl -H "Authorization: bearer $(gcloud auth print-identity-token)" -w "\n" $URL?subreddit=wallstreetbets&type=intraday`

VM :
 - Activate venv : `source venv/bin/activate`
 - Run streaming code with prompt back : `nohup python data_extractor/reddit/streaming_data.py -t submissions -s wallstreetbets`
