import os

import pandas as pd


def compare_expected_and_obtained(
    df_obtained, df_expected, key_columns, comparison_columns
):
    df_merge = pd.merge(
        left=df_obtained,
        right=df_expected,
        how="outer",
        on=key_columns,
        indicator=True,
        suffixes=["_obtained", "_expected"],
    )
    obtained_but_not_expected = df_merge[df_merge["_merge"] == "left_only"][
        key_columns
    ].values.tolist()
    expected_but_not_obtained = df_merge[df_merge["_merge"] == "right_only"][
        key_columns
    ].values.tolist()
    assert (
        len(obtained_but_not_expected) == 0
    ), f"Keys in obtained but not in expected : {obtained_but_not_expected}"
    assert (
        len(expected_but_not_obtained) == 0
    ), f"Keys in expected but not in obtained : {expected_but_not_obtained}"

    df_comparison = df_merge[df_merge["_merge"] == "both"]
    for c in comparison_columns:
        obtained = df_comparison[c + "_obtained"]
        expected = df_comparison[c + "_expected"]
        diff_idx = (1 - expected.isna()).astype("bool") * (expected != obtained)
        different_obtained_expected = df_comparison[diff_idx][
            key_columns
        ].values.tolist()
        assert (
            len(different_obtained_expected) == 0
        ), f"Keys for which obtained and expected {c} are different : {different_obtained_expected}"
