sentiment-analysis-trading
==========================

TODO
====

## GLOBAL :

### CORE
 - [x] Update Crontab for a daily docker cleanup
 - Comment code
 - [ ] Optimize Dockerfile image
   - Alpine is not suited with python !

### ticker detection
    - [x] Add companies' name to ticker detection
    - [ ] Broaden outside US Stock Exchange

Stocks data extraction :
-------------------
    - [ ] Retrieve once a day every stock price per 1m via ALPHA VANGUARD API 

Reddit's extraction :
-------------------

### CORE :
  - [x] Stream new comments & submissions continuously via  `reddit.stream.comments`
  - [x] Deploy above code on VM to run 24/7
  - [x] Dockerize code on VM, and add auto reboot if job fails
  - [x] Change storage type to standard because to heavy pricing
  - [ ] Package code properly to remove the `sys.path.append()` que je ne saurais voir

### Historical extraction :
  - [x] Load new files to BQ table
  - [x] Once a day run a historical extraction on last 2 weeks to get updated submissions
  - [x] Parametrize historical extraction to handle 2 in // just with arguments

### Daily extraction :
  - ~~[x] Upload each 10mn batch to GCS~~
    - ~~[ ] Add feature to extract for weekend & daily subs~~
  - [x] Stream each comment & submisison and upload it to GCS 
    - [x] Add retries to streaming
- [ ] Add the data to postgres for daily alerting
    - [x] Update submissions metadata every 10 minutes 


New data sources :
------------------
- [ ] Look at Alpha Vantage "Market News & Sentiment" which can offer news of specific subjects for FREE 
- [ ] Add Bloomberg & other financial websites
- [ ] Add Crypto's subreddits

R&D :
-----

- [ ] News article directly to GPT
- [ ] Prompt engineering for output as a json
- [ ] HOW TO stream comment to GPT for big subs ?
    - [ ] Add memory to remember which comment shave already been processed ?
    - [ ] Give full comments with their answers and ask model to give sentiment
      for each comment
    - 

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

Docker :
  - Build image from project root`sudo docker build --force-rm -t data_extraction:latest -f apps/Dockerfile .`
  - Retrieve historical data : `sudo docker run -d --name historical_full --restart=on-failure data_extraction python reddit/history_extraction.py --mode=full`
  - (Scheduled daily) Retrieve historical on last 2 weeks : `sudo docker run -d --name historical_ltw --restart=on-failure data_extraction python reddit/history_extraction.py --mode=ltw`
  - (Scheduled every 10mn) Retrieve last submissions metadata : `sudo docker run -d --rm --name submissions_metadata_update data_extraction python reddit/update_submissions_metadata.py`
  - Stream submissions : `sudo docker run -d --name wsb_submissions --restart=unless-stopped data_extraction python reddit/streaming_data.py --type=submissions --subreddit=wallstreetbets`
  - Stream comments : `sudo docker run -d --name wsb_comments --restart=unless-stopped data_extraction python reddit/streaming_data.py --type=comments --subreddit=wallstreetbets`
  - 
  - (daily schedule) Clean all unused images & containers `sudo docker system prune -f`
  - Delete images with pattern  `sudo docker images -a | grep "<none>" | awk '{print $3}' | xargs sudo docker rmi`
    
