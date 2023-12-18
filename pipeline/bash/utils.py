def open_sql_script(
    file_path: str,
    csv_path: str = "",
    csv_type: str = "",
    csv_folder: str = "",
    csv_name: str = "",
    csv_date: str = "",
) -> str:
    with open(file_path) as f:
        query = f.read()
        query = query.replace("{{ params.filepath }}", csv_path)
        query = query.replace("{{ params.filetype }}", csv_type)
        query = query.replace("{{ params.foldername }}", csv_folder)
        query = query.replace("{{ params.filename }}", csv_name)
        query = query.replace("{{ params.filedate }}", csv_date)

    return query
