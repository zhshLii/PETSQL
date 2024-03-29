# coding: utf-8

import random
import requests
import json
import time
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# from settings import SECRETKEYS


class Llama2:

    def __init__(self, keys=None):
        self._key = None

    def __call__(self,
                 prompt,
                 core_pod_ip='10.119.25.63',
                 temperature=0.001,
                 top_p=0.9,
                 max_new_tokens=256,
                 repetition_penalty=1.05,
                 stream=False,
                 *args,
                 **kwargs):
        #替换敏感词
        # prompt_desen=prompt.replace('Master', 'Bachelor')
        # print('-*-S'*20)
        # print(f"Llama2 Prompt:\n{prompt}")
        # print('--' * 30+' Fin Prompt ')

        server = "http://103.177.28.206:8000/api/generate"
        headers = {"Content-Type": "application/json"}
        endpoint = f"http://{core_pod_ip}:2345/generate"  # cci tgi-gpu8

        request_body = {
            "endpoint": endpoint,
            "inputs": prompt,
            "parameters": {
                "temperature": temperature,
                "top_p": top_p,
                "do_sample": True,
                "max_new_tokens": max_new_tokens,
                "top_k": 4,
                "repetition_penalty": repetition_penalty,
                "stop": [
                    #  "</s>",
                    #  "User:",
                ]
            }
        }
        response = requests.post(server,
                                 headers=headers,
                                 json=request_body,
                                 stream=stream)

        time.sleep(0.1)
        if response.status_code == 200:
            try:
                res = response.json()
            except:
                raise Exception('Response can not be parsed !')

            return res["generated_text"]
        else:
            raise Exception('No response returned by SenseNova !')


def parallel_call(inps):
    querys, api = inps[0], inps[1]
    llm = Llama2()
    res = []
    for q in querys:
        out = llm(q, core_pod_ip=api)
        res.append(out)
    return res


if __name__ == '__main__':
    llm = Llama2()
    print(llm('请用一句话解释万有引力'))