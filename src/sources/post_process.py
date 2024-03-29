import re, os
import sqlparse
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--llm", default='codellama')
parser.add_argument("--file", default='')
parser.add_argument("--output", default="")

args = parser.parse_args()

llm = args.llm

def process_duplication(sql):
    sql = sql.strip().split("#")[0]
    return sql

def extract_sql(query, llm='codellama'):
    if llm == "sensechat":
        query = query.replace("### SQL:","").replace("###","").replace("#","")
        if '```SQL ' in query:
            try:
                query = re.findall(r"```SQL(.*?)```", query.replace('\n', ' '))[0]
                # return re.findall(r"```SQL(.*?)```", query.replace('\n', ' '))[0]
            except:
                query = re.findall(r"```SQL(.*?)", query.replace('\n', ' '))[0]
                # return re.findall(r"```SQL(.*?)", query.replace('\n', ' '))[0]
        if '```sql ' in query:

            # print(query)
            try:
                return re.findall(r"```sql(.*?)```", query.replace('\n', ' '))[0]
            except:
                return re.findall(r"```sql(.*?)", query.replace('\n', ' '))[0]
        else:
            return query.replace('\n', '').replace("`","")
    elif llm == "codellama" or llm == "sqlcoder":
        if ';' in query:
            res = re.findall(r'(.*?);', query.replace('\n', ' '))[0].strip()
            # print(res)
            res = res.replace('# ', '').replace('##', '')
            if res.lower().strip().startswith("SELECT".lower()):
                return res.replace('# ', '').replace('##', '')
            else:
                return "SELECT " + res

        else:
            res = query.replace('\n', '').split("###")[0].replace('# ', '').replace('##', '')
            if res.lower().strip().startswith("SELECT".lower()):
                return res.replace('# ', '').replace('##', '')
            else:
                return "SELECT " + res
    elif llm == "puyu":
        res = query.replace("#","")
        if res.lower().strip().startswith("SELECT".lower()):
            return res
        else:
            return "SELECT " + res
    elif llm == 'gpt':
        sql = " ".join(query.replace("\n", " ").split())
        sql = process_duplication(sql)
        # python version should >= 3.8
        if sql.lower().strip().startswith("select"):
            return sql  
        elif sql.startswith(" "):
            return "SELECT" + sql  
        else:
            return "SELECT " + sql 
    else:
        return query

def extract_sql_from_text(text):
    sql_pattern = text.replace("\n", " ").replace("ി", " ").split('~~')
    return sql_pattern


with open(args.file, 'r', encoding='utf-8') as file:
    content = file.readlines()

mid = extract_sql_from_text("\n".join(content))
extracted_query = [sqlparse.format(extract_sql(q, llm), reindent=False) for q in extract_sql_from_text("\n".join(content))]
if args.output:
    with open(args.output, 'w', encoding='utf-8') as file:
        file.write('\n'.join(extracted_query))
else:
    with open(args.file.replace(".txt", "_out.txt") , 'w', encoding='utf-8') as file:
        file.write('\n'.join(extracted_query))
