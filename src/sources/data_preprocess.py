import argparse
import json
import os
import pickle
from pathlib import Path
import sqlite3
from tqdm import tqdm
import random
import sys, os
sys.path.append(os.path.dirname(__file__))
from utils.linking_process import SpiderEncoderV2Preproc
from utils.pretrained_embeddings import GloVe
from utils.datasets.spider import load_tables
# from dataset.process.preprocess_kaggle import gather_questions


def schema_linking_producer(test, train, table, db, dataset_dir, compute_cv_link=True):

    # load data
    test_data = json.load(open(os.path.join(dataset_dir, test)))
    # train_data = json.load(open(os.path.join(dataset_dir, train)))

    # load schemas
    schemas, _ = load_tables([os.path.join(dataset_dir, table)])

    # Backup in-memory copies of all the DBs and create the live connections
    for db_id, schema in schemas.items():
        # sqlite_path = Path(dataset_dir) / db / db_id / f"{db_id}.sqlite"
        sqlite_path = f"{db}/{db_id}/{db_id}.sqlite"
        # print(sqlite_path)
        source: sqlite3.Connection
        with sqlite3.connect(str(sqlite_path)) as source:
            dest = sqlite3.connect(':memory:')
            dest.row_factory = sqlite3.Row
            source.backup(dest)
        schema.connection = dest

    word_emb = GloVe(kind='42B', lemmatize=True)
    linking_processor = SpiderEncoderV2Preproc(dataset_dir,
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=compute_cv_link)

    # build schema-linking
    print("Build test schema-linking ...")
    for data, section in zip([test_data],['test']):
        for item in data:
            db_id = item["db_id"]
            schema = schemas[db_id]
            to_add, validation_info = linking_processor.validate_item(item, schema, section)
            if to_add:
                linking_processor.add_item(item, schema, section, validation_info)

    # save
    linking_processor.save()


import re, json, os

from sql_metadata import Parser

proj_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def add_fk(ppl_test):
    tables_data = json.load(
        open("data/spider/test_data/tables.json", 'r', encoding='utf-8'))
    forekeys = {}
    anno_simddl = {}
    for db in tables_data:
        tables = db["table_names_original"]
        column_names = db["column_names_original"]
        column_types = db["column_types"]
        foreign_keys = db["foreign_keys"]
        sql_tem_all = []
        sql_tem_sim_ddl = []
        for idx, data in enumerate(tables):
            for j, column in enumerate(column_names):
                sql_tem = []
                if idx == column[0]:
                    sql_tem.append(column[0])
                    sql_tem.append(
                        str(column[1]) + " " + str(column_types[j]).upper())
                    sql_tem_all.append(sql_tem)
                    sql_tem_sim_ddl.append([column[0], column[1]])

        simddl_all = []
        for idx, data in enumerate(tables):
            sql_01 = "# " + str(data) + "("
            sql_final_tem = []
            for j, sql_final in enumerate(sql_tem_sim_ddl):
                if idx == sql_final[0]:
                    sql_final_tem.append(sql_final[1])
            sql_01 += ",".join(sql_final_tem) + ")"
            simddl_all.append(sql_01)
        anno_simddl[db["db_id"]] = simddl_all
        forkey = []
        for foreign in foreign_keys:
            vlaus = str(tables[int(
                    column_names[foreign[0]][0])]) + "(" + str(
                column_names[foreign[0]][1]) + ") REFERENCES " + str(tables[int(
                    column_names[foreign[1]][0])]) + "(" + str(
                        column_names[foreign[1]][1]) + ")"
            forkey.append(vlaus)
        forekeys[db["db_id"]] = forkey
    for i in range(len(ppl_test)):
        ppl_test[i]['foreign_key'] = ["\n".join(forekeys[ppl_test[i]["db"]])]
    return ppl_test



def gen_ppl_from_json(ppl_filename='data/ppl_dev.json', model=None):
    tables_data = json.load(
        open(proj_dir + "/data/spider/test_data/tables.json", 'r', encoding='utf-8'))
    dev_data = json.load(
        open(proj_dir + "/data/spider/test_data/dev.json", 'r', encoding='utf-8'))
    ppl_test = []
    for ix, it in enumerate(dev_data):
        ppl_test.append({
            "id": ix,
            "db": it['db_id'],
            "question": it['question'],
            "gold_sql": it['query']
        })
    anno_simddl = {}
    anno = {}
    for db in tables_data:
        tables = db["table_names_original"]
        column_names = db["column_names_original"]
        column_types = db["column_types"]
        sql_tem_all = []
        sql_tem_sim_ddl = []
        for idx, data in enumerate(tables):
            for j, column in enumerate(column_names):
                sql_tem = []
                if idx == column[0]:
                    sql_tem.append(column[0])
                    sql_tem.append(
                        str(column[1]) + " " + str(column_types[j]).upper())
                    sql_tem_all.append(sql_tem)
                    sql_tem_sim_ddl.append([column[0], column[1]])

        # 外键
        for foreign in db["foreign_keys"]:
            vlaus = str(tables[int(
                    column_names[foreign[0]][0])]) + "(" + str(
                column_names[foreign[0]][1]) + ") REFERENCES " + str(tables[int(
                    column_names[foreign[1]][0])]) + "(" + str(
                        column_names[foreign[1]][1]) + ")"
            # print(vlaus)
            sql_tem_all.append([column_names[foreign[0]][0], vlaus])
        # DDL语句
        ddl_all = []
        for idx, data in enumerate(tables):
            # 表名
            sql_01 = "\nCREATE TABLE " + str(data) + "("
            sql_final_tem = []
            for j, sql_final in enumerate(sql_tem_all):
                if idx == sql_final[0]:
                    sql_final_tem.append(sql_final[1])
            sql_01 += ", ".join(sql_final_tem) + ");"
            ddl_all.append(sql_01)
        anno[db["db_id"]] = ddl_all
        
        simddl_all = []
        for idx, data in enumerate(tables):
            sql_01 = "# " + str(data) + "("
            sql_final_tem = []
            for j, sql_final in enumerate(sql_tem_sim_ddl):
                if idx == sql_final[0]:
                    sql_final_tem.append(sql_final[1])
            sql_01 += ", ".join(sql_final_tem) + ")"
            simddl_all.append(sql_01)
        anno_simddl[db["db_id"]] = simddl_all
    for i in range(len(ppl_test)):
        ppl_test[i]['simplified_ddl'] = ";\n".join(
            anno_simddl[ppl_test[i]["db"]]) + ".\n"
        ppl_test[i]['full_ddl'] = "\n".join(anno[ppl_test[i]["db"]]) + '\n'
    ppl_test = add_fk(ppl_test)
    json.dump(ppl_test,
              open(ppl_filename, 'w', encoding='utf-8'),
              ensure_ascii=False,
              indent=4)
    return ppl_test



import re, json, os

from sql_metadata import Parser

    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="data/")
    args = parser.parse_args()

    # merge two training split of Spider
    spider_dir = args.data_dir
    # split1 = "train_spider.json"
    # split2 = "train_others.json"
    # total_train = []
    # for item in json.load(open(os.path.join(spider_dir, split1))):
    #     total_train.append(item)
    # for item in json.load(open(os.path.join(spider_dir, split2))):
    #     total_train.append(item)
    # with open(os.path.join(spider_dir, 'train_spider_and_others.json'), 'w') as f:
    #     json.dump(total_train, f)

    # schema-linking between questions and databases for Spider
    if "test.json" in os.listdir('data/'):
        dev_name = "test.json"
    elif "dev.json" in os.listdir('data/'):
        dev_name = "dev.json"
    else:
        raise Exception("There is no 'test.json' or 'dev.json' in dataset. Please check the file path.")
    spider_dev = dev_name
    print(spider_dev)
    spider_train = ''
    spider_table = 'tables.json'
    spider_db = 'database'
    schema_linking_producer(spider_dev, spider_train, spider_table, spider_db, spider_dir)
