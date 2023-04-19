import requests
import json

#test_sample = json.dumps({'data': [[41.25,41.25,3.56,15,59.81,30,0],[14,14,1.21,15,30.21,30,1]]})
#test_sample = str(test_sample)


#data = {'data':[[41,41,3,15,59,30,0]]}
#data = {'data':[[41.25,41.25,3.56,15.0,59.81,30,0,3]]}
data = {'data':[[1,-0.850470275,0,0,-0.00674112,8]]}

test_sample = str.encode(json.dumps(data))

def test_ml_service(scoreurl, scorekey):
    assert scoreurl != None

    headers = {'Content-Type':'application/json'}

    resp = requests.post(scoreurl, test_sample, headers=headers)
    assert resp.status_code == requests.codes.ok
    assert resp.text != None
    assert resp.headers.get('content-type') == 'application/json'
    assert int(resp.headers.get('Content-Length')) > 0
