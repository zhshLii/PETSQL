import json, os
import jsonlines
import random
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import euclidean_distances
from sql_gen_utils import jaccard_similarity, mask_question_with_schema_linking, sql2skeleton
 

class BasicExampleSelector(object):
    def __init__(self):
        
        with open("src/code_submit/dataset/ppl_train_other.json") as f:
            self.train_json = json.load(f)
        print(f"stored {len(self.train_json)} libray")
        self.train_questions = [sample['question'] for sample in self.train_json]

        with jsonlines.open('src/code_submit/dataset/train_schema-linking.jsonl', 'r') as jsonl_f:
            self.train_schema_jsonl  = [obj for obj in jsonl_f]
        with jsonlines.open('test_schema-linking.jsonl', 'r') as jsonl_f:
            self.test_schema_jsonl  = [obj for obj in jsonl_f]


    def get_examples(self, target, num_example, cross_domain=False):
        pass
   

    def get_schemas_and_preresult(self,):
        self.db_id_to_table_json = dict()
        # try:
        for table_json in json.load(open("data/spider/test_data/tables.json", "r")):
            self.db_id_to_table_json[table_json["db_id"]] = table_json
        # except:
        #     data_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        #     for table_json in json.load(open(data_path + "/data/tables.json", "r")):
        #         self.db_id_to_table_json[table_json["db_id"]] = table_json
        dirs = os.path.dirname(os.path.dirname(__file__))
        with open(dirs + "/raw_out.txt", 'r') as f:
            lines = f.readlines()
            self.preresult = [line.strip() for line in lines]
        
    def get_train_pre_skeleton(self):
        skeletons = []
        queries=  [data["gold_sql"] for data in self.train_json]
        schemas =   [self.db_id_to_table_json[d["db"]] for d in self.train_json]
        for query,schema in zip(queries, schemas):
            skeletons.append(sql2skeleton(query, schema))
    
        for id in range(min(len(self.train_json), len(skeletons))):
            self.train_json[id]["pre_skeleton"] = skeletons[id]
   

    def get_target_pre_skeleton(self, target):
        schema = self.db_id_to_table_json[target["db"]]
        query = self.preresult[target['id']]
        # query = target['gold_sql']
        return sql2skeleton(query, schema)
     
class RandomExampleSelector(BasicExampleSelector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        random.seed(0)
        print(f"set seed for random select")

    def get_examples(self, target, num_example, cross_domain=False):
        train_json = self.train_json
        indexes = list(range(len(train_json)))
        selected_indexes = random.sample(indexes, num_example)
        return [train_json[index] for index in selected_indexes]


class CosineSimilarExampleSelector(BasicExampleSelector):
    def __init__(self, *args, **kwargs):
        super().__init__()

        self.SELECT_MODEL = "./sentence_transformers"
       
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(self.train_questions, show_progress_bar=False)
        print(f"processed embedding for cosine select")
        
    def get_examples(self, target, num_example, cross_domain=False):
        target_embedding = self.bert_model.encode([target["question"]], show_progress_bar=False)
    
        similarities = np.squeeze(cosine_similarity(target_embedding, self.train_embeddings)).tolist()
        pairs = [(similarity, index) for similarity, index in zip(similarities, range(len(similarities)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0], reverse=True)
        top_pairs = list()
        for s, index in pairs_sorted:
            if train_json[index]["question"] == target["question"]:
                continue
            top_pairs.append((index, s))
            if len(top_pairs) >= num_example:
                break
        return [train_json[index] for (index, s) in top_pairs]

class EuclideanDistanceQuestionMaskSelector(BasicExampleSelector):
    def __init__(self, *args, **kwargs):
        super().__init__()

        self.SELECT_MODEL = "./sentence_transformers"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>" # the "<unk>" is the unknown token of all-mpnet-base-v2

        train_mask_questions = mask_question_with_schema_linking(self.train_schema_jsonl, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions, show_progress_bar=False)

    def get_examples(self, target, num_example, cross_domain=False):
        
        target_mask_question = mask_question_with_schema_linking([self.test_schema_jsonl[target['id']]], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question, show_progress_bar=False)

        # find the most similar question in train dataset
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db"]
            if cross_domain and similar_db_id != target["db"]:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]


class EuclideanDistanceQuestionMaskPreSkeletonSimilarThresholdSelector(BasicExampleSelector):
    def __init__(self,  *args, **kwargs):
        super().__init__()

        self.get_schemas_and_preresult()
        self.get_train_pre_skeleton()
        
        self.SELECT_MODEL = "./sentence_transformers"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>"  # the "<unk>" is the unknown token of all-mpnet-base-v2
        self.threshold = 0.85

        train_mask_questions = mask_question_with_schema_linking(self.train_schema_jsonl, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions, show_progress_bar=False)
        


    def get_examples(self, target, num_example, cross_domain=False):
        scope_factor = 100
        target_mask_question = mask_question_with_schema_linking([self.test_schema_jsonl[target['id']]], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question, show_progress_bar=False)

        target["pre_skeleton"] = self.get_target_pre_skeleton(target)
        # find the most similar question in train dataset
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db"]
            if cross_domain and similar_db_id != target["db"]:
                continue
            # Skeleton similarity
            if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) < self.threshold:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example*scope_factor:
                break

        if len(top_pairs) < num_example*scope_factor:
            for d, index in pairs_sorted:
                similar_db_id = train_json[index]["db"]
                if cross_domain and similar_db_id != target["db"]:
                    continue
                # Skeleton similarity
                if jaccard_similarity(train_json[index]["pre_skeleton"], target["pre_skeleton"]) >= self.threshold:
                    continue
                top_pairs.append((index, d))
                if len(top_pairs) >= num_example*scope_factor:
                    break
        return [train_json[index] for (index, d) in top_pairs[:num_example]]


def get_examples_ins(select_type = "Random"):
    if select_type == "CosineSimilar":
        examples_libary = CosineSimilarExampleSelector()
    elif select_type == "Random":
        examples_libary = RandomExampleSelector()
    elif select_type == "Euclidean_mask":
        examples_libary = EuclideanDistanceQuestionMaskSelector()
    elif select_type == "Euclidean_mask_select":
        examples_libary = EuclideanDistanceQuestionMaskPreSkeletonSimilarThresholdSelector()
    return examples_libary


if __name__=='__main__':
    with open("data/ppl_test_input_sql_gen.json") as f:
        input_data = json.load(f)
    target = input_data[0]
    examples_libary = get_examples_ins(target,  "CosineSimilar", 5)
    examples_libary.get_examples(target, 5)
    import pdb; pdb.set_trace()
    