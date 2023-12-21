import csv
import os
import shutil
import subprocess

import pandas as pd
from pandas.tseries.offsets import DateOffset

from dsn_processing._base import Base


def generate_expected_data_files() -> None:
    assert (
        "DSN_PROCESSING_REPOSITORY_PATH" in os.environ
    ), "Missing environment variable DSN_PROCESSING_REPOSITORY_PATH"
    assert (
        "WORKFLOW_SOURCES_DATA_PATH" in os.environ
    ), "Missing environment variable WORKFLOW_SOURCES_DATA_PATH"

    # read expected data excel
    file_name = Base.EXPECTED_DATA_FILE + ".xlsx"
    file_path = os.path.join(
        os.environ["DSN_PROCESSING_REPOSITORY_PATH"], "resources", file_name
    )

    df = pd.read_excel(file_path, sheet_name=None, dtype=str)

    # generate csv
    sheet_list = [
        "Output Entreprises",
        "Output Etablissements",
        "Output Salaries",
        "Output Contrats",
        "Output Postes",
        "Output Activites",
        "Output Contrats_Comparisons",
    ]

    for sheet in sheet_list:
        file_name = f"test_database_expected_data_{sheet.split()[1].lower()}.csv"
        file_path = os.path.join(os.environ["WORKFLOW_SOURCES_DATA_PATH"], file_name)

        # convert excel date format "%Y-%m-%d %H:%M:%S" to "%Y-%m-%d"
        df[sheet] = df[sheet].apply(
            lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S", errors="ignore")
        )

        # save csv
        df[sheet].to_csv(
            file_path,
            index=False,
            quoting=csv.QUOTE_MINIMAL,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )


def generate_input_data_files() -> None:
    assert (
        "DSN_PROCESSING_REPOSITORY_PATH" in os.environ
    ), "Missing environment variable DSN_PROCESSING_REPOSITORY_PATH"
    assert (
        "WORKFLOW_RAW_DATA_PATH" in os.environ
    ), "Missing environment variable WORKFLOW_RAW_DATA_PATH"
    assert (
        "WORKFLOW_TEST_DATA_PATH" in os.environ
    ), "Missing environment variable WORKFLOW_TEST_DATA_PATH"

    file_name = Base.EXPECTED_DATA_FILE + ".xlsx"
    file_path = os.path.join(
        os.environ["DSN_PROCESSING_REPOSITORY_PATH"], "resources", file_name
    )

    df = pd.read_excel(file_path, sheet_name=None, dtype=str)

    # convert excel date format "%Y-%m-%d %H:%M:%S" to "%Y-%m-%d"
    df["Input"] = df["Input"].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S", errors="ignore")
    )
    df = df["Input"]

    def file_date_from_date_chargement(date_chargement: pd.Timestamp):
        if date_chargement.day < 9:
            file_date = date_chargement - DateOffset(months=2)
        else:
            file_date = date_chargement - DateOffset(months=1)
        file_date = file_date.strftime("%Y-%m-01")
        return file_date

    df["file_date"] = df.date_chargement.apply(
        lambda x: file_date_from_date_chargement(x)
    )

    for date in df.file_date.unique():
        print(date, end=" ")
        contrats_to_select = df[["id_contrat", "date_chargement"]].drop_duplicates()

        # 0. décompression du dossier des fichiers bruts
        extract_file = os.path.join(
            os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
            "pipeline",
            "common",
            "extract_archives.sh",
        )
        subprocess.run(
            [
                "bash",
                extract_file,
                date,
                "raw",
            ]
        )

        # 0bis. définition des paths
        raw_folder_name = os.path.join(
            os.environ["WORKFLOW_RAW_DATA_PATH"], f"champollion_{date}"
        )
        light_files_folder = os.path.join(
            os.environ["WORKFLOW_RAW_DATA_PATH"], "test_files"
        )
        if os.path.exists(light_files_folder):
            shutil.rmtree(light_files_folder)
        os.mkdir(light_files_folder)
        target_folder = os.path.join(
            os.environ["WORKFLOW_TEST_DATA_PATH"],
            f"champollion_{date}",
        )
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder)
        os.mkdir(target_folder)

        # 1. on grep les id contrats afin de réécrire un csv allégé
        print("contrats", end=" ")
        raw_file_name = f"champollion_contrat_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(contrats_to_select.id_contrat)
        grep_command = f"grep '{list_to_grep}\|IdContrat' {raw_file_path} --text"
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 2. on lit le csv allégé et on sauvegarde les id nécessaires pour alléger les autres csv
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdContrat.isin(contrats_to_select.id_contrat)]
        assert list(data.DateChargement) == [
            x.strftime("%Y-%m-%d")
            for x in contrats_to_select.set_index("id_contrat")
            .loc[data.IdContrat]
            .date_chargement
        ], "problème de cohérence avec les dates de chargement"
        id_contrat_list = data.IdContrat.unique()
        id_individu_list = data.IdIndividu.unique()
        id_declaration_list = data.IdDeclaration.unique()
        siret_ett_list = data.SIRETEtabUtilisateur.dropna().unique().astype(int)
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 3. on lit le fichier entier 'etablissements' (à cause des id des ETT, on ne peut recourir à une version allégée)
        print("etablissements", end=" ")
        file_name = f"champollion_etablissement_{date}.csv"
        data = pd.read_csv(
            os.path.join(raw_folder_name, file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data["siret"] = data.Siren + data.NIC
        data["siret"] = data["siret"].astype(int)
        data = data.loc[
            (data.IdDeclaration.isin(id_declaration_list))
            | (data.siret.isin(siret_ett_list))
        ]
        id_etablissement_list = data.IdEtablissement.unique()
        data.drop(columns="siret", inplace=True)
        data.to_csv(
            os.path.join(target_folder, file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 4. on allège le fichier individus
        print("individus", end=" ")
        raw_file_name = f"champollion_individu_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_individu_list)
        grep_command = f"grep '{list_to_grep}\|IdIndividu' {raw_file_path} --text"
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 5. on lit et exporte les données souhaitées avec le fichier individus allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdIndividu.isin(id_individu_list)]
        data = data.loc[data.IdEtablissement.isin(id_etablissement_list)]
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 6. on lit le fichier entier contrats fins car il est assez léger
        print("contrats fins", end=" ")
        file_name = f"champollion_contratfin_{date}.csv"
        data = pd.read_csv(
            os.path.join(raw_folder_name, file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdContrat.isin(id_contrat_list)]
        data.to_csv(
            os.path.join(target_folder, file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 7. on allège le fichier contrats changements
        print("contrats changements", end=" ")
        raw_file_name = f"champollion_contratchangement_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_contrat_list)
        grep_command = (
            f"grep '{list_to_grep}\|IdContratChangement' {raw_file_path} --text"
        )
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 8. on lit et exporte les données souhaitées avec le fichier contrats changemenets allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdContrat.isin(id_contrat_list)]
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 9. on allège le fichier individus changements
        print("individus changements", end=" ")
        raw_file_name = f"champollion_individuchangement_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_individu_list)
        grep_command = (
            f"grep '{list_to_grep}\|IdIndividuChangement' {raw_file_path} --text"
        )
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 10. on lit et exporte les données souhaitées avec le fichier individus changements allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdIndividu.isin(id_individu_list)]
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 11. on allège le fichier versements
        print("versements", end=" ")
        raw_file_name = f"champollion_versement_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_individu_list)
        grep_command = f"grep '{list_to_grep}\|IdVersement' {raw_file_path} --text"
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 12. on lit et exporte les données souhaitées avec le fichier versements allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdIndividu.isin(id_individu_list)]
        id_versement_list = data.IdVersement.unique()
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 13. on allège le fichier remunerations
        print("remunerations", end=" ")
        raw_file_name = f"champollion_remuneration_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_versement_list)
        grep_command = f"grep '{list_to_grep}\|IdRemuneration' {raw_file_path} --text"
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 14. on lit et exporte les données souhaitées avec le fichier remunerations allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdVersement.isin(id_versement_list)]
        id_remuneration_list = data.IdRemuneration.unique()
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 15. on allège le fichier activités
        print("activites", end=" ")
        raw_file_name = f"champollion_activite_{date}.csv"
        raw_file_path = os.path.join(raw_folder_name, raw_file_name)
        list_to_grep = "\|".join(id_remuneration_list)
        grep_command = f"grep '{list_to_grep}\|IdActivite' {raw_file_path} --text"
        res = subprocess.check_output(grep_command, shell=True)
        with open(os.path.join(light_files_folder, raw_file_name), "wb") as bin_file:
            bin_file.write(res)

        # 16. on lit et exporte les données souhaitées avec le fichier activités allégé
        data = pd.read_csv(
            os.path.join(light_files_folder, raw_file_name),
            dtype=str,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")
        data = data.loc[data.IdRemuneration.isin(id_remuneration_list)]
        data.to_csv(
            os.path.join(target_folder, raw_file_name),
            index=False,
            sep=";",
            quotechar="|",
            encoding="cp1252",
        )
        print(data.shape, end=" ")

        # 17. suppression du light files folder
        shutil.rmtree(light_files_folder)

        # 18. suppression du dossier des fichiers bruts
        remove_file = os.path.join(
            os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
            "pipeline",
            "common",
            "remove_extracted.sh",
        )
        subprocess.run(
            [
                "bash",
                remove_file,
                date,
                "raw",
            ]
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        action="store_true",
        help="Re-generate test input files if -i is provided.",
    )
    args = parser.parse_args()

    generate_expected_data_files()
    if args.input:
        generate_input_data_files()
