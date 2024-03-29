# coding: utf-8

import random
import requests
import json
import time
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# from settings import SECRETKEYS


class vicuna:

    def __init__(self, keys=None):
        self._key = None

    def __call__(self,
                 prompt,
                 core_pod_ip='10.119.27.61',
                 temperature=0.001,
                 top_p=0.9,
                 max_new_tokens=256,
                 repetition_penalty=1.05,
                 stream=True,
                 *args,
                 **kwargs):

        server = "http://cluster-proxy.sh.sensetime.com:19905/generate"
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
        # response = requests.post(server, headers=headers, data=json.dumps(request_body), timeout=(10, 5))
        time.sleep(0.1)
        # print(response.status_code)
        if response.status_code == 200:
            try:
                res = response.json()
            except:
                raise Exception('Response can not be parsed !')

            return res["generated_text"]
        else:
            raise Exception('No response returned by SQLCoder !')


if __name__ == '__main__':
    llm = vicuna()
    print(llm('请用一句话解释万有引力'))