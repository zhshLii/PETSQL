# coding: utf-8

import random
import requests
import json
import time
import os, sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class SQLCoder:

    def __init__(self, keys=None):
        self._key = None

    def __call__(self,
                 prompt,
                 core_pod_ip='10.119.28.126',
                 temperature=0.001,
                 top_p=0.9,
                 max_new_tokens=256,
                 repetition_penalty=1.05,
                 stream=True,
                 *args,
                 **kwargs):

        url = 'http://cluster-proxy.sh.sensetime.com:19939/generate'
        # context = "<|User|>:"+prompt+"\n<|Bot|>:"
        data = {
            "inputs": prompt,
            "parameters": {
                "do_sample": False,
                "temperature": temperature,
                "top_k": 1,
                "max_new_tokens": max_new_tokens,
                "repetition_penalty": repetition_penalty,
            }
        }
        response = requests.post(url, json=data)

        time.sleep(0.1)
        if response.status_code == 200:
            try:
                res = response.json()
            except:
                raise Exception('Response can not be parsed !')
            return res["generated_text"][0].rstrip("</s>")
        else:
            raise Exception('No response returned by SQLCoder !')


if __name__ == '__main__':
    llm = SQLCoder()
    print(llm('请用一句话解释万有引力'))