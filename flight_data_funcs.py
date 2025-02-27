# importing module
import os
import logging
from zipfile import ZipFile
from datetime import datetime

#!pip install pandas
import pandas as pd

#!pip install scipy
import scipy.io

from IPython.display import clear_output
import plotly_express as px
import plotly.graph_objs as go

from pyspark.sql import SparkSession

__author__ = "Jesus Martinez"
__email__ = "jesus.martinez89@hotmail.com"
__year__ = datetime.today().year
__copyright__ = f"Copyright (C) {__year__} {__author__}"
__license__ = "All rights reserved."
__version__ = "1.0"

# specifying the zip file name
verbose = False
# Creating an object
log = logging.getLogger()


logging.basicConfig(
    format="%(asctime)s %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("newfile.log"), logging.StreamHandler()],
)

def get_aws_credentials():
    with open(os.path.expanduser("~/.aws/credentials")) as f:
        for line in f:
            # print(line.strip().split(' = '))
            try:
                key, val = line.strip().split("=")

                if key == "AWS_SESSION_TOKEN":
                    aws_session_token = val
                elif key == "AWS_SECRET_ACCESS_KEY":
                    AWS_SECRET_ACCESS_KEY = val
                elif key == "AWS_ACCESS_KEY_ID":
                    AWS_ACCESS_KEY_ID = val
            except ValueError:
                pass
    return aws_session_token, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID


def get_paths(paths: list):
    """_summary_

    Args:
        paths (list): _description_

    Returns:
        _type_: _description_
    """
    allpaths = dict()
    for path in paths:
        files = os.listdir(f"extracted_data/{path}")
        allpaths[path] = list()
        for key in files:
            clear_output()
            temp = f"extracted_data/{path}/{key}"
            allpaths[path].append(temp)
    return allpaths


class Unzipping:
    """_summary_"""

    def __init__(self):
        """_summary_"""
        # Create and configure logger
        pass

    def extract_all(self, zipped: ZipFile, tail: str, verbose: bool = False):
        """_summary_

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
        log.info(f"{file_name}")
        tail = file_name.split(".")[0]
        # opening the zip file in READ mode
        with ZipFile(f"zip_data/{file_name}", "r") as zipped:
            extract_all(zipped, tail)


class Parsing:
    """_summary_"""

    def __init__(self, file: str, verbose: bool):
        """_summary_

        Args:
            file (str): _description_
            verbose (bool): _description_
        """
        self.file = file
        self.verbose = verbose
        self.mat = scipy.io.loadmat(file)
        self.mat_keys = list(self.mat.keys())
        keys_to_drop = ["__header__", "__version__", "__globals__"]
        list(map(lambda x: self.mat_keys.remove(x), keys_to_drop))
        self.len_mat_keys = len(self.mat_keys)
        self.fdataframe = pd.DataFrame()

    def print_verbose(self):
        """_summary_"""
        clear_output(wait=True)
        print(self.resolution, self.units, self.coltitle)
        print(self.lendata, (self.lendata**2) * (self.resolution**2))

    def create_series(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        tempseries = pd.Series(
            list(map(lambda x: str(x[0]), self.data)), name=str(self.coltitle[0])
        )
        tempseries.index = list(
            map(
                lambda x: int(x * self.lendata * self.resolution**2),
                range(self.lendata),
            )
        )
        return tempseries

    def parse_mat_column(self, key):
        """_summary_

        Args:
            key (_type_): _description_

        Returns:
            _type_: _description_
        """
        self.data = self.mat[key][0][0][0]
        self.resolution = 1 / self.mat[key][0][0][1][0][0]
        self.units = self.mat[key][0][0][2]
        self.coltitle = self.mat[key][0][0][3]
        self.abbreviation = self.mat[key][0][0][4]
        self.lendata = len(self.data)
        return self.create_series()

    def parse_mat_columns(self, key: str):
        """_summary_

        Args:
            key (str): _description_
        """
        try:
            tempseries = self.parse_mat_column(key)
            self.fdataframe = pd.concat(
                [self.fdataframe, tempseries], axis=1, ignore_index=False
            )

            if self.verbose:
                self.print_verbose()

        except Exception as exception:
            print(exception)

    def mat_to_dataframe(self):
        """_summary_

        Returns:
            _type_: _description_
        """

        list(map(self.parse_mat_columns, self.mat_keys))
        self.fdataframe.sort_index(inplace=True)
        return self.fdataframe


class Drawing:
    def __init__(self):
        pass

    def create_blank_fig(
        self,
        ymin: int = 0,
        ymax: int = 0,
        ydtick: int = 1,
        title: str = "",
        yaxistitle: str = "",
    ):
        """_summary_

        Args:
            ymin (int, optional): _description_. Defaults to 0.
            ymax (int, optional): _description_. Defaults to 0.
            ydtick (int, optional): _description_. Defaults to 1.
            title (str, optional): _description_. Defaults to ''.
            yaxistitle (str, optional): _description_. Defaults to ''.

        Returns:
            _type_: _description_
        """
        return go.Figure(
            layout={
                "template": "none",
                "autosize": True,
                "width": 700,
                "height": 400,
                "title": {"text": title, "x": 0.5, "xanchor": "center"},
                "xaxis": {
                    "color": "black",
                    "gridcolor": "lightgrey",
                    "tickfont": dict(size=9),
                    "tickmode": "linear",
                    "tick0": 0,
                    "dtick": 250,
                    "ticklabelstep": 2,
                },
                "yaxis": {
                    "color": "black",
                    "gridcolor": "lightgrey",
                    "tickfont": dict(size=9),
                    "tickmode": "linear",
                    "tick0": 0,
                    "dtick": ydtick,
                    "ticklabelstep": 5,
                },
                "xaxis_title": "FRAME",
                "yaxis_title": yaxistitle,
                "yaxis_range": [ymin, ymax],
            },
        )

    def create_temp_figure(self, dataframe):
        fig = self.create_blank_fig(
            105, 140, title="TEMP vs FRAME", yaxistitle="EXHAUST GAS TEMPERATURE"
        )

        columns = [
            "EXHAUST GAS TEMPERATURE 1",
            "EXHAUST GAS TEMPERATURE 2",
            "EXHAUST GAS TEMPERATURE 3",
            "EXHAUST GAS TEMPERATURE 4",
        ]

        locs = range(0, dataframe.shape[0], 126)
        egtdataframe = dataframe.loc[locs][columns]

        for col in columns:
            fig.add_traces(
                list(
                    px.line(
                        data_frame=egtdataframe,
                        x=list(map(lambda x: int(x), egtdataframe.index)),
                        y=egtdataframe[col].apply(lambda x: float(x)),
                    ).select_traces()
                )
            )
        return fig

    def create_alt_figure(self, dataframe):
        columns = ["BARO CORRECT ALTITUDE LSP", "RADIO ALTITUDE LSP"]

        locs = range(0, dataframe.shape[0], 126)
        altdataframe = dataframe.loc[locs][columns]
        altdataframe.columns = [
            "BARO CORRECT ALTITUDE LSP",
            "BARO CORRECT ALTITUDE LSP2",
            "RADIO ALTITUDE LSP",
        ]

        fig = self.create_blank_fig(
            840,
            855,
            title="BARO CORRECT ALTITUDE vs FRAME",
            yaxistitle="BARO CORRECT ALTITUDE LSP",
        )

        for col in ["BARO CORRECT ALTITUDE LSP", "BARO CORRECT ALTITUDE LSP2"]:
            fig.add_traces(
                list(
                    px.line(
                        data_frame=altdataframe,
                        x=list(map(lambda x: int(x), altdataframe.index)),
                        y=altdataframe[col].apply(lambda x: float(x)),
                    ).select_traces()
                )
            )
        return fig

    def create_acc_figure(self, dataframe):
        columns = ["VERTICAL ACCELERATION", "LATERAL ACCELERATION"]

        locs = range(0, dataframe.shape[0], 126)
        accdataframe = dataframe.loc[locs][columns]
        accdataframe

        fig = self.create_blank_fig(
            -3.5,
            1.3,
            0.1,
            title="VERT & LAT ACCELERATION vs FRAME",
            yaxistitle="VERT & LAT ACCELERATION",
        )

        for col in columns:
            fig.add_traces(
                list(
                    px.line(
                        data_frame=accdataframe,
                        x=list(map(lambda x: int(x), accdataframe.index)),
                        y=accdataframe[col].apply(lambda x: float(x)),
                    ).select_traces()
                )
            )
        return fig


class Sparking:
    def __init__(self):
        self.aws_session_token, self.aws_sct_acc_key, self.aws_acc_key_id = (
            get_aws_credentials()
        )
        self.spark = (
            SparkSession.builder.appName("IcebergWithSparkAndGlue")
            .config(
                "spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901",
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .getOrCreate()
        )

        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3.access.key", self.aws_acc_key_id
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3.secret.key", self.aws_sct_acc_key
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3.session.token", self.aws_session_token
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", self.aws_acc_key_id
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", self.aws_sct_acc_key
        )
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.session.token", self.aws_session_token
        )
        # spark = SparkSession \
        #        .builder \
        #        .appName("Python Spark basic example") \
        #        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        #        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        #        .getOrCreate()

        # .config("spark.sql.catalogImplementation", "hive") \


class ETLing:
    def __init__(self):
        pass

    def rename_col_ifduplicated(self, dataframe):
        cols = []
        count = 1
        for column in dataframe.columns:
            if column == "BARO CORRECT ALTITUDE LSP":
                cols.append(f"BARO CORRECT ALTITUDE LSP{count}")
                count += 1
                continue
            cols.append(column)
        dataframe.columns = cols
        return dataframe


if __name__ == "__main__":
    pass
