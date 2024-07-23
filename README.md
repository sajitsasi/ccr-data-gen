# Elasticsearch Cross-Cluster Replication Data Generator

This project is designed to generate and index random data into an Elasticsearch Leader index and then verify replication in a Follower index. The script supports both Elastic Cloud and self-managed clusters.

## Prerequisites

- Python 3.x
- Elasticsearch 8.x
- Virtual Environment (recommended)

## Setup

### 1. Clone the Repository

```sh
git clone https://github.com/sajitsasi/ccr-data-gen.git
cd ccr-data-gen
```

### 2. Create and Activate a Virtual Environment

```sh
python -m venv
source venv/bin/activate # On Windows use `venv\Scripts\activate`
```

### 3. Instal Dependencies

```sh
pip install requirements.txt
```

### 4. Setup Environment Variables
```
########################Leader Cluster########################
# Leader Cluster Elastic Cloud info
Leader_ELASTIC_CLOUD_ID=<Your-Leader-Cloud-ID>
# Leader Cluster self-managed info
Leader_ELASTIC_HOST=<Your-Leader-Host>  # Optional if using Cloud ID
Leader_ELASTIC_PORT=9200  # Default port
# Leader Cluster auth info
Leader_ELASTIC_USERNAME=<Your-Leader-Username>
Leader_ELASTIC_PASSWORD=<Your-Leader-Password>
########################Leader Cluster########################

#######################Follower Cluster#######################
# Follower Cluster Elastic Cloud info
Follower_ELASTIC_CLOUD_ID=<Your-Follower-Cloud-ID>
# Follower Cluster self-managed info
Follower_ELASTIC_HOST=<Your-Follower-Host>  # Optional if using Cloud ID
Follower_ELASTIC_PORT=9200  # Default port
# Follower Cluster auth info
Follower_ELASTIC_USERNAME=<Your-Follower-Username>
Follower_ELASTIC_PASSWORD=<Your-Follower-Password>
#######################Follower Cluster#######################

########################Index Settings$#######################
INDEX_NAME=<Your-Index-Name>
EVENTS_PER_SECOND=<Number-of-Events-Per-Second>
COUNT_INDEX_NAME=<Count-Index-Name> # Created on follower Cluster
########################Index Settings$#######################
```

### 5. Run the Script
```
python ./index_ccr_data.py
```


## How it Works

### 1. Index Creation
The script checks if the Leader index exists. If it does not, it is created and prompts the user to set up the corresponding Follower index in the Follower cluster. Note that the index name needs to be the same for both Leader and Follower cluster indices

Another index defined under `COUNT_INDEX_NAME` is created on the Follower cluster to keep track of document counts in the Leader and Follower clusters

### 2. Data Generation
The `generate_document` function creates random documents with fields as specified in the code

### 3. Historical Data Indexing:
The script uses multiple threads to index historical data (30 days back) into the Leader index

### 4. Real-time Data Indexing:
A separate thread continuously indexes real-time data into the Leader index based on the specified events per second (EPS) configured.

### 5. Replication Verification
Another thread separately queries the indices in both the Leader and Follower clusters to get the document count in each. 
Note that since the Follower cluster is queried after the Leader cluster, there may be some instances where the document count on the Follower cluster is higher than the Leader cluster at that time interval.

### 6. Elasticsearch
Note that this has been tested on 8.14, please open an issue if you need this to work for an earlier 8.X version

## License
This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.