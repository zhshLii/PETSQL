import json, tqdm, sys, os, re
from multiprocessing import Pool
import time

proj_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(proj_dir)
from llms import *
from llms import Puyu as puyu
import logging
import tqdm, logging
from sql_metadata import Parser
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--output", default='ppl_dev_add_sl.json')
args = parser.parse_args()

def extract_tab_from_sql(item, sample):
    try:
        linked_tables = Parser(item).tables
    except:
        ddl = sample['simplified_ddl']
        ele_query = " ".join([i for i in re.split(r'[^\w\s*]', item)]).split()
        ele_query = [i.lower() for i in ele_query]
        split_ddl = ddl.split(";\n")
        linked_tables = []
        all_tables = []
        for it in split_ddl:
            all_tables.append(it[2:it.index('(')])
        for one_table in all_tables:
            one_table = one_table.lower()
            if one_table in ele_query:
                linked_tables.append(one_table)
    sample['linked_tables_gpt'] = linked_tables


if __name__ == '__main__':
    input_data = json.load(open(os.path.dirname(__file__) + "/ppl_dev.json", 'r'))
    file_path = os.path.dirname(__file__)
    with open(file_path + '/intermediate_results_only_dont_use_gpt.txt', 'r') as f:
        clm = f.readlines()
    gpt_tab = []
    for ix, it in enumerate(clm):
        extract_tab_from_sql(it, input_data[ix])
    json.dump(input_data, open(os.path.dirname(__file__) + f"/{args.output}", 'w'), indent=4)
    