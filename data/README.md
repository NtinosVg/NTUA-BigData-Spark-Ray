## CSV Data Generation

This folder contains the python and bash scripts necessary to generate a large size CSV dataset with 
classificiation relevant schema and upload it to our HDFS cluster. To run the necessary commands run:


```bash
chmod +x generate_data_to_hdfs.sh
./generate_to_hdfs.sh <num_samples> </path/in/hdfs.csv>
```

This bash script utilizes the *data_generator.py* script to write to stdout and then redirect the output to an hdfs file.


## Graph Data Retrieval-Data Generation

We found real-world graphs in the **KONECT Project** (http://konect.cc/) website as well as the **Stanford SNAP Project website** (https://snap.stanford.edu/)
To load them in our hdfs:
	-	Download the graph tar (using command *wget <link>*)
	-	Extract the data and remove the header (using command *tar -xvf <tar>* and cli operations)
	-	Upload it to HDFS (using command *hdfs dfs -put <filepath> <hdfs:filepath>*)

To generate the Small-World and Scale-Free graphs you can run the *generate_graph* scripts which utilize NetworkX library:
We then need to upload them to the hdfs with the usual command.

```bash
python generate_graph_<sf/sw>.py <num_samples> # Generate a small world/scale free graph with (1000*num_samples) nodes
hdfs dfs -put <filepath> <hdfs:filepath>
```
## Hand Gesture Detection System Dataset

We found real-world images on **Kaggle** ([Kaggle Website](https://www.kaggle.com/)).

### Steps to Load the Dataset into HDFS:

1. Download the dataset zip directly to HDFS using the following command:
   ```bash
   kaggle datasets download marusagar/hand-gesture-detection-system
2. Extract the data into the desired directory:
   ```bash
   unzip hand-gesture-detection-system.zip -d hand-gesture-detection-system


