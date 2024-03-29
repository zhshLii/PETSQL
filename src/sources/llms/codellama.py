# coding: utf-8

import random
import requests
import json
import time
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# from settings import SECRETKEYS


class codellama:

    def __init__(self, keys=None):
        self._key = None

    def __call__(self,
                 prompt,
                 core_pod_ip='10.119.26.210',
                 temperature=0.001,
                 top_p=0.9,
                 max_new_tokens=256,
                 repetition_penalty=1.05,
                 stream=False,
                 *args,
                 **kwargs):

        server = "http://cluster-proxy.sh.sensetime.com:19906/generate"
        # headers = {"Content-Type": "application/json"}
        # endpoint = f"http://{core_pod_ip}:2345/generate"  # cci tgi-gpu8

        # request_body = {
        #     # "endpoint": endpoint,
        #     "inputs": prompt,
        #     "parameters": {
        #         "temperature": temperature,
        #         "top_p": top_p,
        #         "do_sample": True,
        #         "max_new_tokens": max_new_tokens,
        #         "top_k": 4,
        #         "repetition_penalty": repetition_penalty,
        #         "stop": [
        #         #  "</s>",
        #         #  "User:",
        #         ]
        #     }
        # }
        data = {
            "inputs": prompt,
            "parameters": {
                "temperature": temperature,
                "top_p": top_p,
                "do_sample": True,
                "max_new_tokens": max_new_tokens,
                "repetition_penalty": repetition_penalty,
            }
        }
        response = requests.post(server, json=data)

        time.sleep(0.1)
        if response.status_code == 200:
            try:
                res = response.json()
            except:
                raise Exception('Response can not be parsed !')

            return res["generated_text"][0].rstrip("</s>")
        else:
            raise Exception('No response returned by SenseNova !')


def parallel_call(inps):
    querys, api = inps[0], inps[1]
    llm = codellama()
    res = []
    for q in querys:
        out = llm(q, core_pod_ip=api)
        res.append(out)
    return res


if __name__ == '__main__':
    llm = codellama()
    print(llm('请用一句话解释万有引力'))