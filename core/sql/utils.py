DYNAMIC_CSV_TEMPLATE_SEARCHPATH = "{{ params.filepath }}/{{ params.filetype }}/{{ params.foldername }}_{{ params.filedate }}/{{ params.filename }}_{{ params.filedate }}.csv"
STATIC_CSV_TEMPLATE_SEARCHPATH = "{{ params.filepath }}/{{ params.filename }}.csv"


class DataTable:
    def __init__(
        self,
        name: str,
        id_column: str,
        parent_id_columns: list,
        key_column: str,
        auxiliary_columns: list,
        order_columns=[],
    ) -> None:
        assert isinstance(name, str)
        assert isinstance(id_column, str)
        assert isinstance(parent_id_columns, list)
        assert isinstance(key_column, str)
        assert isinstance(auxiliary_columns, list)
        assert isinstance(order_columns, list)

        self.name = name
        self.id_column = id_column
        self.parent_id_columns = parent_id_columns
        self.key_column = key_column
        self.auxiliary_columns = auxiliary_columns
        self.order_columns = order_columns


class SourceTable(DataTable):
    pass


class TargetTable(DataTable):
    pass


class LinkTable:
    def __init__(self, name: str, source_id_column: str, target_id_column: str) -> None:
        assert isinstance(name, str)
        assert isinstance(source_id_column, str)
        assert isinstance(target_id_column, str)

        self.name = name
        self.source_id_column = source_id_column
        self.target_id_column = target_id_column


class MapTable:
    def __init__(
        self,
        table_name: str,
        merged_id_column: str,
        absorbing_id_column: str,
        modification_date_column: str,
    ):
        assert isinstance(table_name, str)
        assert isinstance(merged_id_column, str)
        assert isinstance(absorbing_id_column, str)
        assert isinstance(modification_date_column, str)
        self.table_name = table_name
        self.merged_id_column = merged_id_column
        self.absorbing_id_column = absorbing_id_column
        self.modification_date_column = modification_date_column

    def get_create_table_query(self):
        query = f"""
        DROP TABLE IF EXISTS {self.table_name};
        CREATE TABLE {self.table_name} (
            {self.merged_id_column} BIGINT,
            {self.absorbing_id_column} BIGINT,
            {self.modification_date_column} DATE
        );
        """
        return query

    def get_clean_table_query(self):
        return (
            self.get_remove_isomorphic_mappings_query()
            + "\n\n"
            + self.get_remove_cyclic_mappings_query()
            + "\n\n"
            + self.get_path_update_query()
            + "\n\n"
            + self.get_remove_duplicates()
        )

    def get_remove_isomorphic_mappings_query(self):
        query = f"""
        -- remove isomorphic mappings (a -> a)
        DELETE FROM {self.table_name}
        WHERE {self.merged_id_column} = {self.absorbing_id_column};
        """
        return query

    def get_remove_cyclic_mappings_query(self):
        query = f"""
        DO $$ 
        DECLARE
            rows_deleted INTEGER;
        BEGIN
            LOOP
                -- remove cyclic mappings (a -> b -> c -> a)
                WITH RECURSIVE search_graph({self.merged_id_column},
                                            {self.absorbing_id_column},
                                            {self.modification_date_column}) AS (
                    SELECT
                        {self.merged_id_column},
                        {self.absorbing_id_column},
                        {self.modification_date_column}
                    FROM {self.table_name}
                    UNION ALL
                    SELECT
                        graph.{self.merged_id_column},
                        graph.{self.absorbing_id_column},
                        graph.{self.modification_date_column}
                    FROM {self.table_name} AS graph, search_graph
                    WHERE graph.{self.merged_id_column} = search_graph.{self.absorbing_id_column}
                )

                CYCLE {self.absorbing_id_column} SET is_cycle USING path,

                qualified_paths AS (
                    SELECT
                        *,
                        (SELECT MIN(path_as_array) FROM UNNEST(STRING_TO_ARRAY(TRANSLATE(CAST(path AS VARCHAR), '(){{}}', ''), ',')) AS path_as_array) AS min_vertex_path
                    FROM search_graph
                    WHERE is_cycle IS TRUE
                ),

                mapping_to_delete AS (
                    SELECT
                        {self.merged_id_column},
                        {self.absorbing_id_column}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER(PARTITION BY min_vertex_path ORDER BY {self.modification_date_column} ASC) AS row_num
                        FROM qualified_paths
                    ) AS ord
                    WHERE row_num = 1
                )

                DELETE FROM {self.table_name} AS graph
                USING mapping_to_delete AS del
                WHERE graph.{self.merged_id_column} = del.{self.merged_id_column}
                    AND graph.{self.absorbing_id_column} = del.{self.absorbing_id_column};

                GET DIAGNOSTICS rows_deleted = ROW_COUNT;

                EXIT WHEN rows_deleted = 0;
            END LOOP;
        END $$;
        """
        return query

    def get_remove_duplicates(self):
        query = f"""
        -- keeps only one among a --> b ; a --> b; a --> c ; a --> d etc.
        WITH duplicates AS (
            SELECT
                ctid,
                {self.merged_id_column},
                ROW_NUMBER() OVER (PARTITION BY {self.merged_id_column} ORDER BY {self.modification_date_column} DESC) AS nb
            FROM {self.table_name}
        )

        DELETE FROM {self.table_name}
        USING duplicates
        WHERE duplicates.{self.merged_id_column} = {self.table_name}.{self.merged_id_column}
            AND duplicates.ctid = {self.table_name}.ctid
            AND nb > 1;
        """
        return query

    def get_path_update_query(self):
        query = f"""
        -- update the paths in the map table
        -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> d ; b -> d ; c -> d)
        DO $$
        DECLARE
            counter integer := 0;
            still_updating boolean := true;
        BEGIN
        WHILE still_updating LOOP
            -- update the paths by one step
            -- (ie. for instance a -> b ; b -> c ; c -> d becomes a -> c ; b -> d ; c -> d)
            WITH update_one_step AS (
                UPDATE {self.table_name} AS graph
                SET {self.absorbing_id_column} = graph_bis.{self.absorbing_id_column}
                FROM {self.table_name} AS graph_bis
                WHERE graph_bis.{self.merged_id_column} = graph.{self.absorbing_id_column}
                    AND graph_bis.{self.merged_id_column} != graph_bis.{self.absorbing_id_column}
                RETURNING 1)

            SELECT COUNT(*) > 0 INTO still_updating
            FROM update_one_step;
            -- if while loop is kind of infinite, we raise an error
            IF counter > 10000 THEN
                RAISE EXCEPTION 'infinite loop';
                EXIT;
            END IF;
            counter := counter + 1;
        END LOOP;
        END $$;
        """
        return query


def get_grandparent_load_query(
    source, target, link, add_date_premiere_declaration=False
) -> str:
    assert len(source.parent_id_columns) == 0
    assert len(target.parent_id_columns) == 0
    assert len(source.auxiliary_columns) == len(target.auxiliary_columns)

    query = f"""
        -- On importe les données sans doublon
        WITH sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY {source.key_column} ORDER BY {', '.join(source.order_columns)} DESC) AS row_num
                FROM {source.name}
            ) AS ord
            WHERE row_num = 1
        )

        -- On merge les données entrantes dans la table permanente sur la base de la key_column
        MERGE INTO {target.name} AS target
        USING sourcewithoutduplicates AS source
            ON target.{target.key_column} = source.{source.key_column}
        WHEN NOT MATCHED THEN
            -- Si aucune ligne avec la key_column considérée n'existe, on insert la donnée
            INSERT ({target.key_column},
                    {','.join(target.auxiliary_columns)}
                    {', date_premiere_declaration' if add_date_premiere_declaration else ''})
            VALUES (source.{source.key_column},
                    {','.join([' source.'+ i for i in source.auxiliary_columns])}
                    {', source.date_derniere_declaration' if add_date_premiere_declaration else ''})
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            -- Sinon on update les colonnes auxiliaires
            UPDATE SET {target.key_column} = source.{source.key_column}, {', '.join([
                i +' = source.'+ j for (i,j) in zip(target.auxiliary_columns, source.auxiliary_columns)])};


        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        TRUNCATE TABLE {link.name} CASCADE;
        INSERT INTO {link.name} ({link.source_id_column}, {link.target_id_column})
        SELECT
            source.{source.id_column},
            target.{target.id_column}
        FROM {source.name} AS source
        LEFT JOIN {target.name} AS target
            ON source.{source.key_column} = target.{target.key_column};
    """

    return query


def get_parent_load_query(
    source,
    target,
    link,
    grandparent_link,
    add_date_premiere_declaration=False,
) -> str:
    assert len(source.parent_id_columns) == 1
    assert len(target.parent_id_columns) == 1

    query = f"""
        -- On importe les données avec les id "généalogiques" de la table grandparent permanente
        WITH sourcewithdependences AS (
            SELECT
                {source.name}.*,
                link.{grandparent_link.target_id_column} AS grandparent
            FROM {source.name}
            LEFT JOIN {grandparent_link.name} AS link
                ON link.{grandparent_link.source_id_column} = {source.name}.{source.parent_id_columns[0]}
        ),

        -- On supprime les doublons des données
        sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY grandparent, {source.key_column} ORDER BY {', '.join(source.order_columns)} DESC) AS row_num
                FROM sourcewithdependences
            ) AS ord
            WHERE row_num = 1
        )
        -- On merge les données entrantes dans la table permanente sur la base de la key_column et des id de la table grandparent
        MERGE INTO {target.name} AS target
        USING sourcewithoutduplicates AS source
            ON (target.{target.parent_id_columns[0]} = source.grandparent)
            AND (target.{target.key_column} = source.{source.key_column})
        -- Si la donnée n'est pas présente dans la table, on l'insert
        WHEN NOT MATCHED THEN
            INSERT ({target.parent_id_columns[0]},
                    {target.key_column},
                    {','.join(target.auxiliary_columns)}
                    {', date_premiere_declaration' if add_date_premiere_declaration else ''})
            VALUES (source.grandparent,
                    source.{source.key_column},
                    {','.join([' source.'+ i for i in source.auxiliary_columns])}
                    {', source.date_derniere_declaration' if add_date_premiere_declaration else ''})
        -- Sinon, on update les informations auxiliaires
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            UPDATE SET {target.key_column} = source.{source.key_column}, {', '.join([i+' = source.'+ j for (i,j) in zip(target.auxiliary_columns, source.auxiliary_columns)])};

        -- A nouveau, on considère les données entrantes avec les id "généalogiques" de la table grandparent permanente
        TRUNCATE TABLE {link.name} CASCADE;
        WITH sourcewithdependences AS (
            SELECT
                {source.name}.{source.id_column},
                {source.name}.{source.key_column},
                link.{grandparent_link.target_id_column} AS grandparent
            FROM {source.name}
            LEFT JOIN {grandparent_link.name} AS link
                ON link.{grandparent_link.source_id_column} = {source.name}.{source.parent_id_columns[0]}
        )

        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        INSERT INTO {link.name} ({link.source_id_column}, {link.target_id_column})
        SELECT
            source.{source.id_column},
            target.{target.id_column}
        FROM sourcewithdependences AS source
        LEFT JOIN {target.name} AS target
            ON (target.{target.parent_id_columns[0]} = source.grandparent)
            AND (source.{source.key_column} = target.{target.key_column});
    """

    return query


def get_child_load_query(
    source,
    target,
    link,
    grandparent_link,
    parent_link,
    add_date_premiere_declaration=False,
) -> str:
    assert len(source.parent_id_columns) == 2
    assert len(target.parent_id_columns) == 2

    query = f"""
        -- On importe les données avec les id "généalogiques" des tables parent et grandparent permanentes
        WITH sourcewithdependences AS (
            SELECT
                {source.name}.*,
                link_grandparent.{grandparent_link.target_id_column} AS grandparent,
                link_parent.{parent_link.target_id_column} AS parent
            FROM {source.name}
            LEFT JOIN {grandparent_link.name} AS link_grandparent
                ON link_grandparent.{grandparent_link.source_id_column} = {source.name}.{source.parent_id_columns[0]}
            LEFT JOIN {parent_link.name} AS link_parent
                ON link_parent.{parent_link.source_id_column} = {source.name}.{source.parent_id_columns[1]}
        ),

        -- On supprime les doublons des données
        sourcewithoutduplicates AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY grandparent, parent, {source.key_column} ORDER BY {', '.join(source.order_columns)} DESC) AS row_num
                FROM sourcewithdependences
            ) AS ord
            WHERE row_num = 1
        )

        -- On merge les données entrantes dans la table permanente sur la base de la key_column et des id des tables parent et grandparent
        MERGE INTO {target.name} AS target
        USING sourcewithoutduplicates AS source
            ON (target.{target.parent_id_columns[0]} = source.grandparent)
            AND (target.{target.parent_id_columns[1]} = source.parent)
            AND (target.{target.key_column} = source.{source.key_column})
        -- Si la donnée n'est pas présente dans la table, on l'insert
        WHEN NOT MATCHED THEN
            INSERT ({target.parent_id_columns[0]},
                    {target.parent_id_columns[1]},
                    {target.key_column},
                    {','.join(target.auxiliary_columns)}
                    {', date_premiere_declaration' if add_date_premiere_declaration else ''})
            VALUES (source.grandparent,
                    source.parent,
                    source.{source.key_column},
                    {','.join([' source.'+ i for i in source.auxiliary_columns])}
                    {', source.date_derniere_declaration' if add_date_premiere_declaration else ''})
        -- Sinon, on update les informations auxiliaires
        WHEN MATCHED AND target.date_derniere_declaration <= source.date_derniere_declaration THEN
            UPDATE SET {target.key_column} = source.{source.key_column}, {', '.join([i +' = source.'+ j for (i,j) in zip(target.auxiliary_columns, source.auxiliary_columns)])};

        -- A nouveau, on considère les données entrantes avec les id "généalogiques" des tables grandparent et parent permanente
        TRUNCATE TABLE {link.name} CASCADE;
        WITH sourcewithdependences AS (
            SELECT
                {source.name}.{source.id_column},
                {source.name}.{source.key_column},
                link.{grandparent_link.target_id_column} AS grandparent,
                link_parent.{parent_link.target_id_column} AS parent
            FROM {source.name}
            LEFT JOIN {grandparent_link.name} AS link
                ON link.{grandparent_link.source_id_column} = {source.name}.{source.parent_id_columns[0]}
            LEFT JOIN {parent_link.name} AS link_parent
                ON link_parent.{parent_link.source_id_column} = {source.name}.{source.parent_id_columns[1]}
        )

        -- On enregistre les équivalences entre id dans la table source et id dans la table target
        INSERT INTO {link.name} ({link.source_id_column}, {link.target_id_column})
        SELECT
            source.{source.id_column},
            target.{target.id_column}
        FROM sourcewithdependences AS source
        LEFT JOIN {target.name} AS target
            ON (target.{target.parent_id_columns[0]} = source.grandparent)
            AND (target.{target.parent_id_columns[1]} = source.parent)
            AND (source.{source.key_column} = target.{target.key_column});
    """

    return query


def get_weak_mapping_query(table_name: str, id_column: str, map_table: MapTable):
    query = f"""
        UPDATE {table_name}
        SET {id_column} = m.{map_table.absorbing_id_column}
        FROM {map_table.table_name} AS m
        WHERE m.{map_table.merged_id_column} = {id_column}
            AND m.{map_table.merged_id_column} != m.{map_table.absorbing_id_column};
    """
    return query


def get_strong_mapping_query(table_name: str, id_column: str, map_table: MapTable):
    query = f"""
        DELETE FROM {table_name}
        USING {map_table.table_name} AS m
        WHERE {id_column} = m.{map_table.merged_id_column}
            AND m.{map_table.merged_id_column} != m.{map_table.absorbing_id_column};
    """
    return query


def get_update_activites_from_contrats_merge_query(map_table: MapTable):
    query = f"""
        WITH activites_old AS (
            SELECT
                {map_table.table_name}.{map_table.absorbing_id_column},
                public.activites.mois,
                SUM(public.activites.heures_standards_remunerees) AS heures_standards_remunerees,
                SUM(public.activites.heures_non_remunerees) AS heures_non_remunerees,
                SUM(public.activites.heures_supp_remunerees) AS heures_supp_remunerees,
                MAX(public.activites.date_derniere_declaration) AS date_derniere_declaration
            FROM public.activites
            INNER JOIN {map_table.table_name}
                ON public.activites.contrat_id = {map_table.table_name}.{map_table.merged_id_column}
            GROUP BY {map_table.table_name}.{map_table.absorbing_id_column}, public.activites.mois
        )
        MERGE INTO activites AS activites_new
        USING activites_old
            ON activites_new.contrat_id = activites_old.{map_table.absorbing_id_column}
            AND activites_new.mois = activites_old.mois
        WHEN NOT MATCHED THEN
            INSERT (
                contrat_id,
                mois,
                heures_standards_remunerees,
                heures_non_remunerees,
                heures_supp_remunerees,
                date_derniere_declaration
            ) VALUES (
                activites_old.{map_table.absorbing_id_column},
                activites_old.mois,
                activites_old.heures_standards_remunerees,
                activites_old.heures_non_remunerees,
                activites_old.heures_supp_remunerees,
                activites_old.date_derniere_declaration
            )
        WHEN MATCHED THEN
        UPDATE SET
        heures_standards_remunerees = activites_new.heures_standards_remunerees + activites_old.heures_standards_remunerees,
        heures_non_remunerees = activites_new.heures_non_remunerees + activites_old.heures_non_remunerees,
        heures_supp_remunerees = activites_new.heures_supp_remunerees + activites_old.heures_supp_remunerees,
        date_derniere_declaration = GREATEST(activites_new.date_derniere_declaration, activites_old.date_derniere_declaration);

        DELETE FROM activites
        USING {map_table.table_name}
        WHERE contrat_id = {map_table.table_name}.{map_table.merged_id_column};
    """

    return query


def combine_queries(queries: list):
    res = ""
    for q in queries:
        if len(q) > 0:
            res += "\n" + q
    return res


def check_id_consistency_in_link_table(link_table_name, table_name, field_name):
    subquery = f"""
        DO $$
        DECLARE do_{link_table_name.replace('.','_')}_ids_are_consistent integer;
        BEGIN
            SELECT SUM(CASE WHEN T.{field_name} IS NULL THEN 1 ELSE 0 END)
            INTO do_{link_table_name.replace('.','_')}_ids_are_consistent
            FROM {link_table_name} AS link
            LEFT JOIN {table_name} AS T
                ON link.{field_name} = T.{field_name};

        ASSERT do_{link_table_name.replace('.','_')}_ids_are_consistent = 0, '{link_table_name} has one id referenced which does not exist';
        END$$;
    """
    return subquery


def get_copy_query(table_name, source=True, freeze=True, encoding="WIN1252"):
    query = f"""
        TRUNCATE TABLE {table_name} CASCADE;
        COPY {table_name}
        FROM '{STATIC_CSV_TEMPLATE_SEARCHPATH if source else DYNAMIC_CSV_TEMPLATE_SEARCHPATH}'
        WITH (
            FORMAT 'csv',
            HEADER,
            {'FREEZE,' if freeze else ''}
            DELIMITER ';',
            QUOTE '|',
            ENCODING '{encoding}'
        );
    """

    return query


def update_static_table_query(static_table_name, code_column_name):
    update_table = "update_" + static_table_name
    queries = []
    queries.append(
        f"""
        CREATE TEMPORARY TABLE {update_table} (
            code CHAR(5) PRIMARY KEY NOT NULL,
            libelle VARCHAR
        );
    """
    )
    queries.append(get_copy_query(update_table))
    queries.append(
        f"""
        MERGE INTO public.{static_table_name}
        USING {update_table}
            ON public.{static_table_name}.{code_column_name} = {update_table}.code
        WHEN MATCHED THEN
            UPDATE SET libelle = {update_table}.libelle
        WHEN NOT MATCHED THEN
            INSERT (
                {code_column_name},
                libelle
            ) VALUES (
                {update_table}.code,
                {update_table}.libelle
            );
    """
    )
    queries.append(
        f"""
        DROP TABLE {update_table};
        """
    )

    return combine_queries(queries)


def remove_old_data(table_name: str, retention_period: int):
    assert retention_period >= 36

    query = f"""
        DELETE FROM public.{table_name} AS ligne
        USING public.chargement_donnees AS base
        WHERE EXTRACT(YEAR FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) * 12 + EXTRACT(MONTH FROM AGE(base.dernier_mois_de_declaration_integre, ligne.date_derniere_declaration)) >= {retention_period};
    """
    return query


def shift_anonymization(
    table_name: str, id_col: str, selection_clause: str, columns: list
):
    row_jump = "\n"
    query = f"""
        CREATE TABLE anonymous.{table_name} AS

        WITH upper_bound AS (
            SELECT MAX({id_col}) AS max_id
            FROM public.{table_name}
        ),

        unnamed_data AS (
            SELECT
                upper_bound.max_id - {table_name}.{id_col} + 1 AS twin_id,
                {table_name}.*
            FROM public.{table_name}, upper_bound
            WHERE {selection_clause}
        ),

        renamed_data AS (
            SELECT 
                {f',{row_jump}                '.join(['twin_data.'+ c if v == "twin" else 'unnamed_data.'+ c for c, v in columns])}
            FROM unnamed_data
            INNER JOIN public.{table_name} AS twin_data
                ON twin_data.{id_col} = unnamed_data.twin_id
        )

        SELECT * FROM renamed_data;
    """
    return query
