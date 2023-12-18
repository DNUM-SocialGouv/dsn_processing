import json
import os
import re

import pandas as pd

config = json.load(
    open(
        os.path.join(
            os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
            "core",
            "python",
            "raw_files_management",
            "raw_files_config.json",
        ),
        "r",
    )
)


def get_folder_path(filedate: str, filetype: str):
    return os.path.join(
        os.environ["WORKFLOW_RAW_DATA_PATH"]
        if filetype == "raw"
        else os.environ["WORKFLOW_TEST_DATA_PATH"],
        "champollion_" + filedate,
    )


def get_file_path(file: str, filedate: str, filetype: str):
    return os.path.join(
        get_folder_path(filedate, filetype),
        file + "_" + filedate + ".csv",
    )


def check_files_sizes(filedate: str, filetype: str) -> None:
    if filetype != "raw":
        return
    for file in config["required_files"]:
        file_path = get_file_path(file, filedate, filetype)
        size = os.path.getsize(file_path)
        assert (size >= config["files_sizes"][file][0]) & (
            size <= config["files_sizes"][file][1]
        ), f"File {file_path} is not of consistent size ({size} not in \
            [{config['files_sizes'][file][0]}, {config['files_sizes'][file][1]}])."


def check_folder_exist(filedate: str, filetype: str) -> None:
    folder_path = get_folder_path(filedate, filetype)
    assert os.path.exists(folder_path), f"Folder {folder_path} does not exist."


def check_files_exist(filedate: str, filetype: str) -> None:
    if filetype != "raw":
        return
    for file in config["required_files"]:
        file_path = get_file_path(file, filedate, filetype)
        assert os.path.exists(file_path), f"File {file_path} does not exist."


def check_files_columns(filedate: str, filetype: str) -> None:
    for file in config["required_files"]:
        file_path = get_file_path(file, filedate, filetype)
        if os.path.exists(file_path):
            df = pd.read_csv(
                filepath_or_buffer=file_path,
                delimiter=config["delimiter"],
                quotechar=config["qualifier"],
                encoding=config["encoding"],
                header=0,
                nrows=0,
            )
            assert (
                len(df.columns) == len(config["required_columns"][file])
                and (df.columns == config["required_columns"][file]).all()
            ), f"File {file_path} does not contain the expected columns."


def check_files_delimiter(filedate: str, filetype: str) -> None:
    for file in config["required_files"]:
        file_path = get_file_path(file, filedate, filetype)
        if os.path.exists(file_path):
            df = pd.read_csv(
                filepath_or_buffer=file_path,
                delimiter="%",
                quotechar="@",
                encoding=config["encoding"],
                header=None,
                nrows=1,
            )
            assert (
                config["delimiter"] in df.iloc[0, 0]
            ), f"Delimiter of {file_path} is not {config['delimiter']}."


def check_files_qualifier(filedate: str, filetype: str) -> None:
    if filetype != "raw":
        return
    for file in config["required_files"]:
        file_path = get_file_path(file, filedate, filetype)
        if os.path.exists(file_path):
            df = pd.read_csv(
                filepath_or_buffer=file_path,
                delimiter="%",
                quotechar="@",
                encoding=config["encoding"],
                header=0,
                nrows=1,
            )
            assert (
                config["qualifier"] in df.iloc[0, 0]
                if re.search(r"[a-zA-Z]+", str(df.iloc[0, 0]))
                else True
            ), f"Text qualifier of {file_path} is not {config['qualifier']}."


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--filedate",
        required=True,
        help="filedate of declaration.",
    )
    parser.add_argument(
        "-t",
        "--filetype",
        required=True,
        help="filetype (raw or test).",
    )
    args = parser.parse_args()
    filedate = args.filedate
    filetype = args.filetype

    check_folder_exist(filedate, filetype)
    check_files_exist(filedate, filetype)
    check_files_sizes(filedate, filetype)
    check_files_qualifier(filedate, filetype)
    check_files_delimiter(filedate, filetype)
    check_files_columns(filedate, filetype)
