# -*- coding: utf-8 -*-
#特别注意：参数传递时去除“<>”符号！
import requests
import json
def send_messag_example():
    resp = requests.post(("http://api.weimi.cc/2/sms/send.html?type=json"),
    data={
        "uid": "<enter your UID>",
        "pas": "<enter your UID Pass>",
        "mob": "<enter your mobiles>",
        "con": "【微米】您的验证码是：610912，3分钟内有效。如非您本人操作，可忽略本消息。",
        "type": "json"
    },timeout=3 , verify=False)

    result=json.loads(resp.content.decode())
    print(result)
if __name__ == "__main__":
    send_messag_example()
#注意：以上参数传入时不包括“<>”符号