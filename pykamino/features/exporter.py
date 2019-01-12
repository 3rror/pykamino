import os.path
import pandas


def _dataframe_to_csv(dataframe, csv_path):
    # If file exists, don't add csv headers
    add_csv_header = not os.path.isfile(csv_path)
    dataframe.to_csv(csv_path, mode="a", header=add_csv_header, index=False)


def features_to_csv(features_list, path):
    df = pandas.DataFrame(features_list)
    _dataframe_to_csv(df, path)
