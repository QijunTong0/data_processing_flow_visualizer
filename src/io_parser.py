import re
import json
import sql_metadata

json_path = "../config/data_io_format.json"
with open(json_path) as f:
    io_format = json.load(f)


def python_data_io_parser(py_ipynb_file_path):
    """
    Args:
        py_ipynb_file_path(str) : a full path of .py or .ipynb file to parse
    Returns:
        tuple(List,List): pair of inputed and outputed filenames lists
    Note:
        The input and output format are defined in /config/data_io_format.json.
    """

    with open(py_ipynb_file_path) as f:
        code = f.read()
    # remove space,indention etc...
    code = re.sub("[\s]+", "", code)
    # if notebook is inputed, then remove metadata
    if ".ipynb" in (py_ipynb_file_path):
        code = re.sub("\\\\\"", "\"", code)
    input_patterns = io_format["python"]["input_pattern"]
    output_patterns = io_format["python"]["output_pattern"]
    suffix = io_format["python"]["suffix"]

    input_data = []
    output_data = []

    for input_pattern in input_patterns:
        input_data.extend(re.findall(input_pattern+suffix, code))

    for output_pattern in output_patterns:
        output_data.extend(re.findall(output_pattern+suffix, code))
    return (input_data, output_data)


def sql_data_io_parser(sql_file_path):
    """
    Args:
        sql_file_path(str) : a full path of .sql file to parse
    Returns:
        tuple(List,List): pair of inputed and outputed tables lists
    Note:
        The supported table identifing formats are depend on sql-metadata library
    """
    with open(sql_file_path) as f:
        code = f.read()

    output_patterns = io_format["sql"]["output_pattern"]
    suffix = io_format["sql"]["suffix"]

    all_tables = sql_metadata.Parser(code).tables
    output_tables = []
    for output_pattern in output_patterns:
        output_tables.extend(re.findall(output_pattern+suffix, code))
    input_tables = [
        table for table in all_tables if table not in output_tables]
    return(input_tables, output_tables)
