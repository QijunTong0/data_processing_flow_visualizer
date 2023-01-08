import argparse
import src.dpfv_lib

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='DPF_visualizer_exe.py',
        usage='if you want to build a data processing flow graph of a repository : python exe.py repository_name',
    )
    parser.add_argument('-r', '--repository_path', type=str,
                        help='The repository to build graph.')
    parser.add_argument('-s', '--show_directory', default=True, type=int,
                        help='If True,contains full path at nodes nome in graph.')

    args = parser.parse_args()
    instance = src.dpfv_lib.data_processing_flow_dag(
        repository_path=args.repository_path, show_directory=args.show_directory)
    instance.build_py_graph()
    instance.build_sql_graph()
    instance.draw_graphs()
    instance.detect_cycle()
    instance.detect_conflict_writing_data()
    print("Done.")
