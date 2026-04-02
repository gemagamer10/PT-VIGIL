#!/usr/bin/env python3
"""
Testa diferentes endpoints da API base.gov.pt
"""
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "pt-PT,pt;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.base.gov.pt/Base4/pt/pesquisa/",
}

def testar(label, url, params=None):
    print(f"\n{'='*60}")
    print(f"TESTE: {label}")
    print(f"URL: {url}")
    if params:
        print(f"PARAMS: {params}")
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        print(f"STATUS: {r.status_code}")
        print(f"CONTENT-TYPE: {r.headers.get('content-type','?')}")
        print(f"TAMANHO: {len(r.text)} chars")
        print(f"INÍCIO DA RESPOSTA: {r.text[:300]!r}")
    except Exception as e:
        print(f"ERRO: {e}")

# Teste 1: endpoint original do scraper
testar(
    "Endpoint original (Base4)",
    "https://www.base.gov.pt/Base4/pt/resultados/",
    {
        "type": "search_contratos",
        "query": "",
        "baseType": "contratoSearch",
        "entidade": "Câmara Municipal de Lisboa",
        "page": 1,
        "pageSize": 10,
    }
)

# Teste 2: API REST base2
testar(
    "REST base2 — contrato individual",
    "https://www.base.gov.pt/base2/rest/contratos/1000000"
)

# Teste 3: endpoint alternativo (API mais recente)
testar(
    "Endpoint alternativo v2",
    "https://www.base.gov.pt/Base4/pt/resultados/",
    {
        "type": "contratos",
        "entidade": "Câmara Municipal de Lisboa",
        "page": 1,
        "pageSize": 10,
    }
)

# Teste 4: API pública documentada
testar(
    "API pública /api/contratos",
    "https://www.base.gov.pt/base2/rest/contratos",
    {
        "entidade": "1",
        "page": "0",
        "pageSize": "10",
    }
)

print("\n" + "="*60)
print("FIM DOS TESTES")
