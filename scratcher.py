import requests

cookies = {
    'isMyIgusAuthenticated': 'false',
    '_gcl_au': '1.1.1527379673.1721995112',
    '_gid': 'GA1.2.1169537396.1721995112',
    'specificga': 'GA1.2.266961493.1721995112',
    'specificga_gid': 'GA1.2.2001295533.1721995112',
    'rollupga': 'GA1.2.266961493.1721995112',
    'rollupga_gid': 'GA1.2.1043486748.1721995112',
    '_fbp': 'fb.1.1721995112439.953121183370838249',
    '_evga_1ed9': '{%22uuid%22:%227ff84dd6f044c522%22}',
    '_sfid_820f': '{%22anonymousId%22:%227ff84dd6f044c522%22%2C%22consents%22:[]}',
    'server': '1',
    'ASP.NET_SessionId': 'jlaegdqvjfw4wpkmjgywud2h',
    'CookiesInfo': '1',
    'stg_returning_visitor': 'Fri%2C%2026%20Jul%202024%2012:00:30%20GMT',
    'liveagent_invite_rejected_5732X000000Gr8i': 'true',
    'cartArticleCount': '0',
    'DomainUserMapping': 'isLocalPriceDomain=1&uc=in&ud=www.igus.in&ucn=India',
    'hubspotutk': '7a3769d6c26aeac8762d6b952ee44f43',
    '__hssrc': '1',
    'Global_SessionId': 'ivv3vckcoplyil3cywygkyui',
    'stg_traffic_source_priority': '1',
    '_pk_ses.6cffafdd-f01c-4312-8dae-3ec1d1be0771.532d': '*',
    '__hstc': '191717877.7a3769d6c26aeac8762d6b952ee44f43.1722063650126.1722098745027.1722148738260.4',
    '_ga_Y5KY8VD3FH': 'GS1.1.1722148735.6.1.1722149418.46.0.0',
    '_ga_7YN4G92YEY': 'GS1.1.1722148735.6.1.1722149418.46.0.0',
    '_ga': 'GA1.1.266961493.1721995112',
    '_pk_id.6cffafdd-f01c-4312-8dae-3ec1d1be0771.532d': 'b6428f75009b2560.1721995119.6.1722149419.1722148736.',
    '_uetsid': '63dc04204b4611efa04d5d32f14e489f',
    '_uetvid': '63dc40204b4611ef8ef5430ea6a6ea1e',
    '_ga_2EYPKJ5T8T': 'GS1.2.1722148736.6.1.1722149420.0.0.0',
    '__hssc': '191717877.13.1722148738260',
    'stg_last_interaction': 'Sun%2C%2028%20Jul%202024%2006:51:01%20GMT',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'If-None-Match': '"3lcc6ye0c461zh"',
    'Referer': 'https://www.igus.com/e-motors/electric-motors',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'purpose': 'prefetch',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'x-nextjs-data': '1',
}

params = {
    'categoryL1': 'e-motors',
    'categoryL2': 'electric-motors',
    'params': '2',
}

response = requests.get(
    'https://www.igus.com/_next/data/cY02SCKGDBHEB_7VM8W1H/en-US/e-motors/electric-motors/2.json',
    params=params,
    cookies=cookies,
    headers=headers,
)

print(response.text)
