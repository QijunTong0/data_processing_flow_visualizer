import re
from pathlib import Path
import json
import sql_metadata
import os
import glob
import matplotlib.pyplot as plt
import networkx as nx
import pygraphviz as pgv
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


class data_processing_flow_dag:
    """ 
    This class conducts a DAG of data processing flow. 

    """

    def __init__(self, repository_path, show_directory=True):
        """
         show_directory: bool,default:True
           if False,the directory of the file is ignored.
        """
        self.show_directory = show_directory
        self.repository_path = repository_path
        self.py_graph = None
        self.sql_graph = None

    def build_py_graph(self):
        """
        Build data processing flow graph according to py files
        Returns:
            None
        """
        self.py_graph = pgv.AGraph(directed=True)
        all_files = [p for p in glob.glob(
            self.repository_path+'/**', recursive=True) if os.path.isfile(p)]
        for file in all_files:
            if Path(file).suffix in [".py", ".ipynb"]:
                input_data, output_data = python_data_io_parser(file)
            else:
                continue
            # convert backslash in Windows path to forwardslask
            file = file.replace("\\", "/").replace("\n", "/n")
            if self.show_directory == False:
                file = Path(file).name
                input_data = [Path(_).name for _ in input_data]
                output_data = [Path(_).name for _ in output_data]

            # add nodes and edges on graph
            self.py_graph.add_node(file, style="filled",
                                   fillcolor="#ffc0cb", shape="box")
            self.py_graph.add_nodes_from(
                input_data+output_data, style="filled", fillcolor="#87cefa")
            edge_in = [(i, file) for i in input_data]
            edge_out = [(file, o) for o in output_data]
            self.py_graph.add_edges_from(edge_in+edge_out)

    def build_sql_graph(self):
        """
        Build data processing flow graph according to sql files
        Returns:
            None
        """
        self.sql_graph = pgv.AGraph(directed=True)
        all_files = [p for p in glob.glob(
            self.repository_path+'/**', recursive=True) if os.path.isfile(p)]
        for file in all_files:
            if Path(file).suffix in [".sql"]:
                input_data, output_data = sql_data_io_parser(file)
            else:
                continue
            # convert backslash in Windows path to forwardslask
            file = file.replace("\\", "/").replace("\n", "/n")
            if self.show_directory == False:
                file = Path(file).name
                input_data = [Path(_).name for _ in input_data]
                output_data = [Path(_).name for _ in output_data]

            # add nodes and edges on graph
            self.sql_graph.add_node(
                file, style="filled", color="#ffc0cb", shape="box")
            self.sql_graph.add_nodes_from(
                input_data+output_data, style="filled", color="#87cefa")
            edge_in = [(i, file) for i in input_data]
            edge_out = [(file, o) for o in output_data]
            self.sql_graph.add_edges_from(edge_in+edge_out)

    def draw_graphs(self, save_path=None):
        """
        Draw data processing flow graph in which the color of file nodes and data nodes are distrinct with graphviz.
        Default : The graph is saved at data_processing_flow_graph folder the repository directory
        """
        if save_path == None:
            save_path = self.repository_path+"/"
        if self.py_graph:
            self.py_graph.draw(
                save_path+Path(self.repository_path).name+"_py.svg", prog="dot")
        if self.sql_graph:
            self.sql_graph.draw(
                save_path+Path(self.repository_path).name+"_sql.svg", prog="dot")
