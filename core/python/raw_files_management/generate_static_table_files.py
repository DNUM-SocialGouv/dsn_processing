import csv
import io
import os
import re
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from PyPDF2 import PdfReader

from dsn_processing._base import Base

# main sources for 2023 :
# https://www.net-entreprises.fr/nomenclatures-dsn-p23v01/
# https://www.net-entreprises.fr/media/documentation/dsn-cahier-technique-2023.1.pdf
# They must be updated each year
CSV_NET_ENTREPRISES = "https://portail.net-entreprises.fr/nmcl/telechargeFichierCsv/"


def get_pdf_text_from_url(url: str, page: int) -> str:
    remote_file = urlopen(Request(url)).read()
    memory_file = io.BytesIO(remote_file)
    pdf_reader = PdfReader(memory_file)
    text = pdf_reader.pages[page].extract_text()

    return text


def extract_data_from_dsn_documentation(text: str) -> list:
    data = list()
    pattern = re.compile("\n\s*\d{2} - ")
    matchs = list(pattern.finditer(text))
    for i in range(len(matchs)):
        start = matchs[i].start()
        if i == len(matchs) - 1:
            stop = len(text)
            row = text[start:stop].split("\n \n")[0].split(" - ")
        else:
            stop = matchs[i + 1].start()
            row = text[start:stop].split(" - ")
        code = row[0].strip()
        libelle = row[1].strip()
        data.append([code, libelle])

    return data


def generate_conventions_collectives_file(year: str) -> None:
    url = f"{CSV_NET_ENTREPRISES}NEODESP{year}-IDCC-P{year}V01.csv"

    df = pd.read_csv(
        url,
        skiprows=[0],
        dtype=str,
        sep=";",
        encoding="latin1",
        quotechar='"',
        header=None,
    )

    df.columns = ["code", "libelle", "date_debut_validite", "date_fin_validite"]
    df = df[["code", "libelle"]]
    code_list = np.setdiff1d(
        np.array([format(i, "04") for i in range(10000)]), df["code"]
    )
    df_deleted_codes_IDCC = pd.DataFrame(
        {
            "code": code_list,
            "libelle": "[Absente dans la version 2023 des IDCC]",
        }
    )
    df = pd.concat([df, df_deleted_codes_IDCC]).sort_values("code")
    df["libelle"] = df["libelle"].str.replace("\n", "").str.strip()

    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"],
        Base.CONVENTIONS_COLLECTIVES_FILE + ".csv",
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_ALL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )

    print(f"Created file {file_path}")


def generate_naf_file(year: str) -> None:
    url = f"{CSV_NET_ENTREPRISES}NEODESP{year}-NAF-P{year}V01.csv"

    df = pd.read_csv(
        url,
        skiprows=[0],
        sep=";",
        dtype=str,
        encoding="latin1",
        quotechar='"',
        header=None,
    )

    df.columns = ["code", "libelle"]
    df = df[["code", "libelle"]]
    df["libelle"] = df["libelle"].str.replace("\n", "").str.strip()

    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"],
        Base.NAF_FILE + ".csv",
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_ALL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )

    print(f"Created file {file_path}")


def generate_natures_contrats_file(year: str) -> None:
    url = f"https://www.net-entreprises.fr/media/documentation/dsn-cahier-technique-20{year}.1.pdf"

    text = get_pdf_text_from_url(url, 175)
    data = extract_data_from_dsn_documentation(text)
    df = pd.DataFrame(data, columns=["code", "libelle"])

    df["libelle"] = df["libelle"].str.replace("\n", "").str.strip()

    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"],
        Base.NATURES_CONTRATS_FILE + ".csv",
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_ALL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )

    print(f"Created file {file_path}")


def generate_motifs_recours_file(year: str) -> None:
    url = f"https://www.net-entreprises.fr/media/documentation/dsn-cahier-technique-20{year}.1.pdf"

    text = get_pdf_text_from_url(url, 182)
    data = extract_data_from_dsn_documentation(text)
    df = pd.DataFrame(data, columns=["code", "libelle"])

    df["libelle"] = df["libelle"].str.replace("\n", "").str.strip()

    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"],
        Base.MOTIFS_RECOURS_FILE + ".csv",
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_ALL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )

    print(f"Created file {file_path}")


def generate_categories_juridiques_insee_file() -> None:
    url = "https://www.insee.fr/fr/statistiques/fichier/2028129/cj_septembre_2022.xls"

    df = pd.read_excel(
        url,
        dtype=str,
        sheet_name="Niveau III",
        skiprows=[0, 1, 2, 3],
    )

    df.columns = ["code", "libelle"]
    # Hand-adding of past insee codes
    # source : https://www.insee.fr/fr/information/2028129
    df_deleted_insee_codes = pd.DataFrame(
        [
            ["5498", "[Supprimé] SARL unipersonnelle"],
            [
                "5720",
                "[Supprimé] Société par actions simplifiées à associé unique ou société par actions simplifiée unipersonnelle",
            ],
            ["6588", "[Supprimé] Société civile laitière"],
            ["1100", "[Supprimé] Artisan-commerçant"],
            ["1200", "[Supprimé] Commerçant"],
            ["1300", "[Supprimé] Artisan"],
            ["1400", "[Supprimé] Officier public ou ministériel"],
            ["1500", "[Supprimé] Profession libérale"],
            ["1600", "[Supprimé] Exploitant agricole"],
            ["1700", "[Supprimé] Agent commercial"],
            ["1800", "[Supprimé] Associé gérant de Société"],
            ["1900", "[Supprimé] (Autre) Personne physique"],
            ["6412", "[Supprimé] Société mutuelle d'assurance"],
            ["6413", "[Supprimé] Union de sociétés mutuelles d'assurances"],
            ["6414", "[Supprimé] Autre société non commerciale d'assurance"],
            [
                "7510",
                "[Supprimé] Service d'une collectivité locale à comptabilité distincte",
            ],
            [
                "7520",
                "[Supprimé] Régie d'une collectivité locale non dotée de la personnalité morale",
            ],
            [
                "7130",
                "[Supprimé] Service du ministère des Postes et Télécommunications",
            ],
        ],
        columns=["code", "libelle"],
    )
    df = pd.concat([df, df_deleted_insee_codes]).sort_values("code")
    df_unknown_codes_insee = pd.DataFrame(
        {
            "code": np.setdiff1d(
                np.array([format(i, "04") for i in range(10000)]), df["code"]
            ),
            "libelle": "[Inconnu]",
        }
    )
    df = pd.concat([df, df_unknown_codes_insee]).sort_values("code")
    df["libelle"] = df["libelle"].str.replace("\n", "").str.strip()

    file_path = os.path.join(
        os.environ["WORKFLOW_SOURCES_DATA_PATH"],
        Base.CATEGORIES_JURIDIQUES_INSEE_FILE + ".csv",
    )

    df.to_csv(
        file_path,
        index=False,
        quoting=csv.QUOTE_ALL,
        sep=";",
        quotechar="|",
        encoding="cp1252",
    )

    print(f"Created file {file_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y", "--year", type=int, help="The current year (format yy).", required=True
    )
    args = parser.parse_args()
    year = args.year

    generate_conventions_collectives_file(year=year)
    generate_naf_file(year=year)
    generate_natures_contrats_file(year=year)
    generate_categories_juridiques_insee_file()
    generate_motifs_recours_file(year=year)
