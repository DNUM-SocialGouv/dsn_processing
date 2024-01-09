import datetime as dt
import os

import numpy as np
import pandas as pd
import pytest

from dsn_processing._sqlconnector import SQLConnector
from dsn_processing.pipeline.bash.utils import open_sql_script
from dsn_processing.tests.utils import compare_expected_and_obtained


@pytest.fixture(scope="session")
def db_conn():
    connector = SQLConnector(verbose=True, log_level="debug")
    connector.open_connection(
        database="test",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=os.environ["POSTGRES_PORT"],
        host=os.environ["POSTGRES_HOST"],
    )

    yield connector


def test_entreprises_table(db_conn):
    df_obtained = db_conn.execute_query(
        """
        SELECT *
        FROM public.entreprises
        """,
        return_dataframe=True,
    )
    df_expected = db_conn.execute_query(
        """
        SELECT *
        FROM test.expected_entreprises
        """,
        return_dataframe=True,
    )

    compare_expected_and_obtained(
        df_obtained=df_obtained,
        df_expected=df_expected,
        key_columns=["entreprise_key"],
        comparison_columns=["raison_sociale", "date_derniere_declaration"],
    )


def test_etablissements_table(db_conn):
    df_obtained = db_conn.execute_query(
        """
        SELECT EN.entreprise_key, ET.*
        FROM public.etablissements AS ET
        INNER JOIN public.entreprises AS EN
        ON ET.entreprise_id = EN.entreprise_id
        """,
        return_dataframe=True,
    )
    df_expected = db_conn.execute_query(
        """
        SELECT EN.entreprise_key, ET.*
        FROM test.expected_etablissements AS ET
        INNER JOIN test.expected_entreprises AS EN
        ON ET.entreprise_id = EN.entreprise_id
        """,
        return_dataframe=True,
    )

    compare_expected_and_obtained(
        df_obtained=df_obtained,
        df_expected=df_expected,
        key_columns=["entreprise_key", "etablissement_key"],
        comparison_columns=[
            "siege_social",
            "ouvert",
            "enseigne",
            "adresse",
            "complement_adresse",
            "code_postal",
            "commune",
            "code_categorie_juridique_insee",
            "code_naf",
            "code_convention_collective",
            "date_derniere_declaration",
        ],
    )


def test_salaries_table(db_conn):
    df_obtained = db_conn.execute_query(
        """
        SELECT E.etablissement_key, S.*
        FROM public.salaries AS S
        INNER JOIN public.etablissements AS E
        ON S.etablissement_id = E.etablissement_id
        """,
        return_dataframe=True,
    )
    df_expected = db_conn.execute_query(
        """
        SELECT E.etablissement_key, S.*
        FROM test.expected_salaries AS S
        INNER JOIN test.expected_etablissements AS E
        ON S.etablissement_id = E.etablissement_id
        """,
        return_dataframe=True,
    )

    compare_expected_and_obtained(
        df_obtained=df_obtained,
        df_expected=df_expected,
        key_columns=["etablissement_key", "salarie_key"],
        comparison_columns=[
            "nir",
            "nom_famille",
            "nom_usage",
            "prenoms",
            "date_naissance",
            "lieu_naissance",
            "date_derniere_declaration",
        ],
    )


def test_contrats_table(db_conn):
    df_obtained = db_conn.execute_query(
        """
        SELECT
        E.etablissement_key,
        S.salarie_key,
        C.*,
        P.poste_key,
        EEU.etablissement_key AS etu_key,
        ECT.contrat_key AS ett_contrat_key
        FROM public.contrats AS C
        INNER JOIN public.salaries AS S
        ON S.salarie_id = C.salarie_id
        INNER JOIN public.etablissements AS E
        ON C.etablissement_id = E.etablissement_id
        INNER JOIN public.postes AS P
        ON P.poste_id= C.poste_id
        LEFT JOIN public.etablissements AS EEU
        ON EEU.etablissement_id = C.etu_id
        LEFT JOIN public.contrats AS ECT
        ON ECT.contrat_id = C.ett_contrat_id
        """,
        return_dataframe=True,
    )
    df_expected = db_conn.execute_query(
        """
        SELECT
        E.etablissement_key,
        S.salarie_key,
        C.*,
        ECC.date_fin_effective_comparison,
        P.poste_key,
        EEU.etablissement_key AS etu_key,
        ECT.contrat_key AS ett_contrat_key
        FROM test.expected_contrats AS C
        LEFT JOIN test.expected_salaries AS S
        ON S.salarie_id = C.salarie_id
        LEFT JOIN test.expected_etablissements AS E
        ON C.etablissement_id = E.etablissement_id
        LEFT JOIN test.expected_postes AS P
        ON P.poste_id= C.poste_id
        LEFT JOIN test.expected_contrats_comparisons AS ECC
        ON ECC.contrat_id = C.contrat_id
        LEFT JOIN test.expected_etablissements AS EEU
        ON EEU.etablissement_id = C.etu_id
        LEFT JOIN test.expected_contrats AS ECT
        ON ECT.contrat_id = C.ett_contrat_id
        """,
        return_dataframe=True,
    )

    compare_expected_and_obtained(
        df_obtained=df_obtained,
        df_expected=df_expected,
        key_columns=["etablissement_key", "salarie_key", "contrat_key"],
        comparison_columns=[
            "date_debut",
            "date_debut_effective",
            "numero",
            "poste_key",
            "code_nature_contrat",
            "code_convention_collective",
            "code_motif_recours",
            "date_fin_previsionnelle",
            "etu_key",
            "ett_contrat_key",
            "date_derniere_declaration",
        ],
    )

    # Cas particulier pour la comparaison "souple" des dates de fin effectives (0 = pas de comparaison, 1 = égalité stricte, 2 = égalité souple)
    kc = ["etablissement_key", "salarie_key", "contrat_key"]
    df_merge = pd.merge(
        left=df_obtained,
        right=df_expected,
        how="outer",
        on=kc,
        indicator=True,
        suffixes=["_obtained", "_expected"],
    )
    df_comparison = df_merge[df_merge["_merge"] == "both"]
    for c in ["statut_fin", "date_fin_effective"]:
        obtained = df_comparison[c + "_obtained"]
        expected = df_comparison[c + "_expected"]
        diff_idx = (1 - expected.isna()).astype("bool") * (
            (df_comparison["date_fin_effective_comparison"] == 2)
            * (expected > obtained)
            + (df_comparison["date_fin_effective_comparison"] == 1)
            * (expected != obtained)
        )
        different_obtained_expected = df_comparison[diff_idx][kc].values.tolist()
        assert (
            len(different_obtained_expected) == 0
        ), f"Keys for which obtained and expected {c} are different : {different_obtained_expected}"


def test_data_augmentation_keys_changes(db_conn):
    query = """
    TRUNCATE TABLE raw.raw_changements_salaries;
    TRUNCATE TABLE raw.raw_changements_contrats;

    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (1, 1, '2022-01-01', '9876543211234', NULL, NULL, NULL, '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (2, 1, '2022-01-01', '9876543211234', 'BRUNO', NULL, NULL, '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (3, 1, '2022-01-01', '9876543211234', NULL, 'VERONIQUE', NULL, '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (4, 1, '2022-01-01', '9876543211234', NULL, NULL, '16991977', '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (5, 1, '2022-01-01', NULL, 'DURENCEAU', NULL, NULL, '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (6, 1, '2022-01-01', NULL, NULL, 'Joséphine', NULL, '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (7, 2, '2022-01-01', NULL, NULL, NULL, '06051989', '2023-01-01', 20230101, '2023-01-01', 20230101);
    INSERT INTO raw.raw_changements_salaries (idindividuchangement, idindividu, datemodification, anciennir, nomfamille, prenoms, datenaissance, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (8, 2, '2022-01-01', NULL, NULL, 'Elisabeth', '06051990', '2023-01-01', 20230101, '2023-01-01', 20230101);
    
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (1, 1, '2022-01-01', '12345678912345', NULL, NULL, '2022-02-01', 20220201, '2022-02-01', 20220201);
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (2, 1, '2022-01-02', NULL, '001', '2021-07-01', '2022-02-01', 20220201, '2022-02-01', 20220201);
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (3, 2, '2022-01-01', '123', '000', '2012-01-01', '2022-02-01', 20220201, '2022-02-01', 20220201);
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (4, 2, '2022-01-02', NULL, NULL, '2015-07-01', '2022-02-01', 20220201, '2022-02-01', 20220201);
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (5, 3, '2022-01-02', '12345678912345', NULL, '2015-07-01', '2022-02-01', 20220201, '2022-02-01', 20220201);
    INSERT INTO raw.raw_changements_contrats (idcontratchangement, idcontrat, datemodification, siretetab, numero, datedebut, datedeclaration, iddeclaration, datechargement, iddatechargement) VALUES (6, 3, '2022-01-02', NULL, 'numero', NULL, '2022-02-01', 20220201, '2022-02-01', 20220201);
    """

    db_conn.execute_query(query, commit=True)

    db_conn.execute_query(
        open_sql_script(
            file_path=os.path.join(
                os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
                "core",
                "sql",
                "monthly_integration",
                "transform_changements_salaries.sql",
            )
        ),
        commit=True,
    )
    db_conn.execute_query(
        open_sql_script(
            file_path=os.path.join(
                os.environ["DSN_PROCESSING_REPOSITORY_PATH"],
                "core",
                "sql",
                "monthly_integration",
                "transform_changements_contrats.sql",
            )
        ),
        commit=True,
    )

    res_changements_salaries = db_conn.execute_query(
        """SELECT * FROM source.source_changements_salaries ORDER BY source_salarie_id, ancien_nir, ancien_nom_famille, anciens_prenoms, ancienne_date_naissance""",
        return_dataframe=True,
    )
    res_changements_contrats = db_conn.execute_query(
        """SELECT * FROM source.source_changements_contrats ORDER BY source_contrat_id, siret_ancien_employeur, ancien_numero, ancienne_date_debut""",
        return_dataframe=True,
    )

    expected_res_changements_salaries = pd.DataFrame.from_dict(
        {
            "source_salarie_id": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 2,
                9: 1,
                10: 1,
                11: 1,
                12: 2,
            },
            "ancien_nir": {
                0: np.nan,
                1: 9876543211234.0,
                2: np.nan,
                3: 9876543211234.0,
                4: 9876543211234.0,
                5: np.nan,
                6: 9876543211234.0,
                7: 9876543211234.0,
                8: np.nan,
                9: 9876543211234.0,
                10: 9876543211234.0,
                11: 9876543211234.0,
                12: np.nan,
            },
            "ancien_nom_famille": {
                0: "DURENCEAU",
                1: "BRUNO",
                2: None,
                3: "BRUNO",
                4: "BRUNO",
                5: "DURENCEAU",
                6: None,
                7: "BRUNO",
                8: None,
                9: None,
                10: None,
                11: None,
                12: None,
            },
            "anciens_prenoms": {
                0: "JOSÉPHINE",
                1: None,
                2: "JOSÉPHINE",
                3: "VERONIQUE",
                4: "VERONIQUE",
                5: None,
                6: "VERONIQUE",
                7: None,
                8: None,
                9: "VERONIQUE",
                10: None,
                11: None,
                12: "ELISABETH",
            },
            "ancienne_date_naissance": {
                0: None,
                1: None,
                2: None,
                3: "16-99-1977",
                4: None,
                5: None,
                6: None,
                7: "16-99-1977",
                8: "06-05-1989",
                9: "16-99-1977",
                10: None,
                11: "16-99-1977",
                12: "06-05-1990",
            },
        }
    )
    expected_res_changements_salaries.sort_values(
        by=[
            "source_salarie_id",
            "ancien_nir",
            "ancien_nom_famille",
            "anciens_prenoms",
            "ancienne_date_naissance",
        ],
        inplace=True,
    )
    expected_res_changements_salaries.reset_index(drop=True, inplace=True)

    expected_res_changements_contrats = pd.DataFrame.from_dict(
        {
            "source_contrat_id": {0: 2, 1: 1, 2: 1, 3: 1, 4: 2, 5: 3, 6: 3, 7: 3},
            "siret_ancien_employeur": {
                0: np.nan,
                1: 12345678912345.0,
                2: np.nan,
                3: 12345678912345.0,
                4: 123.0,
                5: 12345678912345.0,
                6: np.nan,
                7: 12345678912345.0,
            },
            "ancien_numero": {
                0: None,
                1: None,
                2: "001",
                3: "001",
                4: "000",
                5: None,
                6: "NUMERO",
                7: "NUMERO",
            },
            "ancienne_date_debut": {
                0: dt.date(2015, 7, 1),
                1: None,
                2: dt.date(2021, 7, 1),
                3: dt.date(2021, 7, 1),
                4: dt.date(2012, 1, 1),
                5: dt.date(2015, 7, 1),
                6: None,
                7: dt.date(2015, 7, 1),
            },
        }
    )

    expected_res_changements_contrats.sort_values(
        by=[
            "source_contrat_id",
            "siret_ancien_employeur",
            "ancien_numero",
            "ancienne_date_debut",
        ],
        inplace=True,
    )
    expected_res_changements_contrats.reset_index(drop=True, inplace=True)

    assert res_changements_salaries[
        [
            "source_salarie_id",
            "ancien_nir",
            "ancien_nom_famille",
            "anciens_prenoms",
            "ancienne_date_naissance",
        ]
    ].equals(expected_res_changements_salaries)

    assert res_changements_contrats[
        [
            "source_contrat_id",
            "siret_ancien_employeur",
            "ancien_numero",
            "ancienne_date_debut",
        ]
    ].equals(expected_res_changements_contrats)


def test_holidays_calendar(db_conn):
    res = db_conn.execute_query(
        "SELECT public_holiday FROM public.daily_calendar WHERE full_date='2000-12-25';",
        return_dataframe=True,
    )

    assert len(res) > 0
    assert res["public_holiday"].all()
