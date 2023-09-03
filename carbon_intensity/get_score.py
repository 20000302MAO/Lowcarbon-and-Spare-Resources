import requests


def ci_fw24h(from_time):
    headers = {
        'Accept': 'application/json'
    }
    request_str = 'https://api.carbonintensity.org.uk/intensity/'+from_time+'/fw24h'
    print(request_str)

    r = requests.get(request_str, params={}, headers=headers)

    print(r.json())


ci_fw24h('2023-06-25T00:00Z')