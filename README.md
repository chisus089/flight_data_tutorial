<p align="center">
<img src="imgs/logo2.png" alt="bigbyte" width="30%">

# ETL for flight data recorder files to AWS S3 datalake using python
</p>

A tutorial on how to extract-transform-load the NASA Sample Flight Dataset, as downloaded from https://c3.ndc.nasa.gov/dashlink/projects/85/ at the moment of writing this doc into AWS S3.

A personal AWS account is required in order to save to AWS. Credentials also need to be configured before being able to use AWS CLI. More instructions available at: 
https://docs.aws.amazon.com/cli/latest/reference/


The dataset can still be manipulated locally without an AWS account.


The tutorial focuses on: 
   1. getting the dataset to a local computer,
   2. saving the local dataset to AWS S3 object storage in raw format,
   3. reading the data from AWS S3
   4. transform .mat files into dataframe
   5. save dataframe to AWS S3 as parquet files
   6. explore parquet files using AWS Athena
  
## Table Of Contents

- [Prepare NASA dataset](#Extraction)
    - [Get zipped files to your computer](#GetFiles)
    - [Unzip locally](#UnzipFiles)
    - [OPTIONAL: Save files to S3 in raw format using AWS S3 sync](#AWS-S3-Sync)
- [Transform NASA dataset](#transform)
    - [Read raw format files from s3 or from local](#read-s3-boto3)
    - [Parse raw files into a structured dataframe](#parse-files)
    - [OPTIONAL: Save dataframe to S3 as parquet using AWS Boto3 and AWS Data Wrangler](#write-s3-parquet)
- [Analyze NASA dataset](#analyze)
    - [Use AWS athena to query big flight data](#aws-athena)

## Extraction

#### A note on flight data files.

Flight data files format may change depending on the flight data recorder model/manufacturer/configurations. The files obtained from unzipping NASA flight dataset are similar to those of real flight data recorders but not identical. If real flight data used, adjust code accordingly using available data recorder/airframe information. 

### GetFiles


1. Create a new folder in your working directory

    `mkdir flight_data`

    `cd flight_data`

2. Download the file download_flight_data.sh from https://c3.ndc.nasa.gov/dashlink/projects/85/. You can directly download it here: https://c3.ndc.nasa.gov/dashlink/static/media/project/download_flight_data_2.sh

    `wget https://c3.ndc.nasa.gov/dashlink/static/media/project/download_flight_data_2.sh`

3. Create a new subfolder called zip_data. Change to that directory. Execute the bash file in your linux terminal to download the full dataset
    `mkdir zip_data`

    `cd zip_data`

    `bash ../dowload_flight_data.sh`

![alt text](imgs/image1.png)

4. Files will start downloading, it may take some time for the full dataset to download. You may only download a few files if you like. Check the progress on your working folder an interrupt the download at anytime if require.
    
![alt text](imgs/image2.png)

### UnzipFiles

1. Unzip some Tail_xxx_x.zip files downloaded from NASA website. You can use the next code or any other tool to unzip files to a new folder within your `flight_data` folder called `extracted_data`. 

```
import os
import logging
from zipfile import ZipFile

def extract_all(self, zipped: ZipFile, tail: str, verbose: bool = False):
    """_Extracts all files from a folder_
    Args:
        zipped (ZipFile): _description_
        tail (str): _description_
        verbose (bool, optional): _description_. Defaults to False.
    """
    if verbose:
        zip.printdir()
    # extracting all the files
    outpath = f"extracted_data/{tail}"
    size = len(zipped.filelist)
    log.info(f"Extracting {size} files to {outpath}...")
    zipped.extractall(outpath)
    log.info("Done!")

def extract_from_zip(self, file_name: str):
    """_summary_
    Args:
        file_name (str): _description_
    """
    log.info(f"extracting {file_name}")
    tail = file_name.split(".")[0]
    # opening the zip file in READ mode
    with ZipFile(f"zip_data/{file_name}", "r") as zipped:
        extract_all(zipped, tail)

if __name__ == "__main__":

    filelist = os.listdir("zip_data/")
    list(map(extract_from_zip, filelist))

```

2. After extracting, you should be able to see a folder for each zip file, each folder with a few `.MAT` files

![alt text](imgs/image3.png)

![alt text](imgs/image4.png)

### AWS-S3-Sync

OPTIONAL: At this moment, raw files may be backed up in a cloud object storage. Further analysis can continue either from local raw files or cloud backed up raw files. 

Backing up raw files would be a good practice if looking to build a more robust data governed solution. Local is recommended for quick, cheap, viable exploratory analysis. 

Unzipping all .MAT files from the full dataset to your local may saturate your local storage and affect performance. Deleting unzipped .MAT files after analysis is recommended if working local.

1. Use AWS S3 sync command to copy your local raw files folder to your previously configured personal AWS S3 account. More info at: https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html

2. Create a folder in S3 using AWS web console.

3. Execute `aws s3 sync` command. See example below.

    Sintaxis:

    `$ aws s3 sync <source> <target> [--options]`

    Example:

    `$ aws s3 sync . s3://aws-root-main/flight_database/raw/`


    ![alt text](imgs/image5.png)



### transform
### read-s3-boto3
### parse-files
### write-s3-parquet
### analyze
### aws-athena







## Demo
Here is a working live demo :  https://youtube.com/


## To-do
- Add instructions on how to encrypt data before saving to AWS.
- Use cloud visualization tools for data visualizations.


## Team

<img src="https://avatars.githubusercontent.com/u/39705698?v=4 " alt="Jesus Martinez" width="30%">

[Jesus Jorge Martinez Rios](https://www.linkedin.com/in/jesusjmartinezr/) 

## [License](https://github.com/chisus089/flight_data_tutorial/blob/main/LICENSE)

© Jesus Jorge Martinez Rios 

[jesus.martinez89@hotmail.com](jesus.martinez89@hotmail.com)

