#!/usr/bin/env python3
"""
PT-VIGIL PIPELINE v3 — dados.gov.pt (IMPIC / Portal Base)
==========================================================
Fonte oficial: https://dados.gov.pt/en/datasets/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/
Dados: 2012-2026, actualizados de 15 em 15 dias pelo IMPIC.

SEM pip install — usa só bibliotecas padrão do Python 3.
(openpyxl instalado automaticamente se necessário)

Uso rápido (descarrega 2022-2026, ~160MB):
    python3 pt_vigil_pipeline.py

Tudo desde 2012:
    python3 pt_vigil_pipeline.py --anos todos

Anos específicos:
    python3 pt_vigil_pipeline.py --anos 2023 2024 2025

Só processar (se já descarregaste):
    python3 pt_vigil_pipeline.py --so-processar

Pasta de saída:  ./pt_vigil_dados/
"""

import sys
import os
import re
import csv
import json
import time
import zipfile
import sqlite3
import logging
import argparse
import io
from pathlib import Path
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ─── Instalar openpyxl se necessário ──────────────────────────────────────────
def garantir_openpyxl():
    try:
        import openpyxl
        return True
    except ImportError:
        print("⚙  openpyxl não encontrado. A instalar automaticamente...")
        import subprocess
        for cmd in [
            [sys.executable, "-m", "pip", "install", "--quiet", "openpyxl"],
            [sys.executable, "-m", "pip", "install", "--quiet", "--break-system-packages", "openpyxl"],
            ["pip3", "install", "--quiet", "openpyxl"],
            ["pip3", "install", "--quiet", "--break-system-packages", "openpyxl"],
        ]:
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=60)
                if r.returncode == 0:
                    print("   ✓ openpyxl instalado")
                    return True
            except Exception:
                continue
        print("✗  Não foi possível instalar openpyxl automaticamente.")
        print("   Corre manualmente: pip3 install openpyxl")
        print("   Ou: pip3 install --break-system-packages openpyxl")
        return False

# ─── Estrutura de pastas ──────────────────────────────────────────────────────
PASTA       = Path("pt_vigil_dados")
PASTA_ZIP   = PASTA / "zip"        # ZIPs originais
PASTA_XLSX  = PASTA / "xlsx"       # XLSXs extraídos / descarregados
PASTA_PROC  = PASTA / "processado" # CSVs processados
DB_PATH     = PASTA / "pt_vigil.db"

# ─── URLs DIRECTOS do dados.gov.pt (Permalinks estáveis) ─────────────────────
# Ficheiros ZIP e XLSX por ano, publicados pelo IMPIC, domínio público.
# Permalinks em: https://dados.gov.pt/en/datasets/r/<uuid>
FICHEIROS = {
    "2012": ("https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/20260322-123630/contratos2012.xlsx", "xlsx"),
    "2013": ("https://dados.gov.pt/en/datasets/r/a2c94863-9520-4b74-8dc3-2c6f553e5225", "xlsx"),
    "2014": ("https://dados.gov.pt/en/datasets/r/e13c5660-5e96-4b83-a9ab-37a3ded88b38", "xlsx"),
    "2015": ("https://dados.gov.pt/en/datasets/r/1ba9b60d-e6e1-4606-9b99-e0bca0b9c00b", "xlsx"),
    "2016": ("https://dados.gov.pt/en/datasets/r/59c2f9e2-dc75-4d8b-8ff5-d25b4e5d4c7a", "xlsx"),
    "2017": ("https://dados.gov.pt/en/datasets/r/ec59acd8-e3aa-4019-9c82-d6fa0e8f4bcc", "xlsx"),
    "2018": ("https://dados.gov.pt/en/datasets/r/e12a0cf6-ef8a-43ae-9e80-d7f5c5cfb1ed", "xlsx"),
    "2019": ("https://dados.gov.pt/en/datasets/r/63b6a9a1-8ef0-43a3-b1dd-5dcf4fba7bfe", "xlsx"),
    "2020": ("https://dados.gov.pt/en/datasets/r/7b10e69d-39aa-412e-8b04-1dd84c0aa9a3", "xlsx"),
    "2021": ("https://dados.gov.pt/en/datasets/r/36ab7e2e-15e0-487c-870d-82e7498a9b20", "xlsx"),
    "2022": ("https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/20260322-123724/contratos2022.xlsx", "xlsx"),
    "2023": ("https://dados.gov.pt/en/datasets/r/3dba3c92-ce50-4d0e-aab2-ffd5dc4d2d3d", "xlsx"),
    "2024": ("https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/20260322-123742/contratos2024.xlsx", "xlsx"),
    "2025": ("https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/20260322-123752/contratos2025.xlsx", "xlsx"),
    "2026": ("https://dados.gov.pt/s/resources/contratos-publicos-portal-base-impic-contratos-de-2012-a-2026/20260329-091005/contratos2026.zip", "zip"),
}

# Permalinks alternativos (fallback se URL directo mudar)
PERMALINKS = {
    "2026": "https://dados.gov.pt/en/datasets/r/dc5d0543-6e01-4f7e-8709-bf3815812a08",
    "2025": "https://dados.gov.pt/en/datasets/r/4943fdb9-f946-4b6a-b98d-7a61499e37e6",
    "2024": "https://dados.gov.pt/en/datasets/r/f51ae381-0d85-4617-b943-cc2447beca1d",
    "2022": "https://dados.gov.pt/en/datasets/r/a2f49d48-a1e6-4349-b394-6417af1208f1",
    "2012": "https://dados.gov.pt/en/datasets/r/f6b81a07-9490-4b1a-9dfe-8ae89ab7d192",
}

ANOS_RECENTES = ["2022", "2023", "2024", "2025", "2026"]
ANOS_TODOS    = list(FICHEIROS.keys())

HEADERS_HTTP = {
    "User-Agent": "Mozilla/5.0 (compatible; PT-VIGIL/3.0)",
    "Accept": "*/*",
}

# ─── Logging ──────────────────────────────────────────────────────────────────
def setup_logging():
    PASTA.mkdir(exist_ok=True)
    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(PASTA / "pipeline.log", encoding="utf-8"))
    except Exception:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )

log = logging.getLogger("pt-vigil")

# ─── Schema BD ────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS contratos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    objeto          TEXT,
    entidade_adj    TEXT,
    adjudicataria   TEXT,
    adj_nif         TEXT,
    tipo_contrato   TEXT,
    tipo_proc       TEXT,
    preco           REAL,
    preco_efetivo   REAL,
    data_pub        TEXT,
    data_contrato   TEXT,
    cpv             TEXT,
    distrito        TEXT,
    ano             TEXT,
    ajuste_direto   INTEGER DEFAULT 0,
    score_risco     REAL DEFAULT 0,
    nivel_risco     TEXT DEFAULT 'baixo',
    flags           TEXT DEFAULT '[]',
    fonte_ano       TEXT,
    atualizado      TEXT,
    UNIQUE(objeto, entidade_adj, adjudicataria, data_pub)
);
CREATE TABLE IF NOT EXISTS entidades (
    nome            TEXT PRIMARY KEY,
    nif             TEXT,
    total_contratos INTEGER DEFAULT 0,
    total_valor     REAL DEFAULT 0,
    score_avg       REAL DEFAULT 0,
    score_max       REAL DEFAULT 0,
    pct_ajuste      REAL DEFAULT 0,
    atualizado      TEXT
);
CREATE TABLE IF NOT EXISTS adjudicantes (
    nome            TEXT PRIMARY KEY,
    total_contratos INTEGER DEFAULT 0,
    total_valor     REAL DEFAULT 0,
    score_avg       REAL DEFAULT 0,
    pct_ajuste      REAL DEFAULT 0,
    atualizado      TEXT
);
CREATE INDEX IF NOT EXISTS idx_risco ON contratos(score_risco DESC);
CREATE INDEX IF NOT EXISTS idx_nivel ON contratos(nivel_risco);
CREATE INDEX IF NOT EXISTS idx_ano   ON contratos(ano);
CREATE INDEX IF NOT EXISTS idx_adjt  ON contratos(adjudicataria);
CREATE INDEX IF NOT EXISTS idx_adj   ON contratos(entidade_adj);
"""

# ─── Mapeamento de colunas do XLSX do IMPIC ───────────────────────────────────
MAPA_COLUNAS = {
    "objeto":    ["Objeto do Contrato", "Objeto", "objeto", "objectoContrato", "objecto"],
    "tipo_proc": ["Tipo de Procedimento", "Tipo Procedimento", "tipoprocedimento"],
    "tipo_cont": ["Tipo(s) de Contrato", "Tipo Contrato", "tipoContrato"],
    "cpv":       ["CPV"],
    "adj":       ["Entidade(s) Adjudicante(s)", "Entidade Adjudicante", "Adjudicante", "adjudicante"],
    "adjt":      ["Entidade(s) Adjudicatária(s)", "Entidade Adjudicatária", "Entidade Adjudicataria", "adjudicatarios"],
    "preco":     ["Preço Contratual", "Preco Contratual", "Valor", "precoContratual"],
    "data_pub":  ["Data de Publicação", "Data Publicacao", "Data Publicação", "dataPublicacao"],
    "data_cont": ["Data de Celebração do Contrato", "Data Celebração", "Data Contrato", "dataCelebracaoContrato"],
    "preco_ef":  ["Preço Total Efetivo", "Preco Total Efetivo", "Preço Efetivo", "PrecoTotalEfetivo"],
    "local":     ["Local de Execução", "Local Execucao", "Local", "LocalExecucao"],
}

# ─── Parsing ──────────────────────────────────────────────────────────────────
def parse_preco(v):
    if v is None:
        return 0.0
    s = re.sub(r'[€\s\xa0]', '', str(v).strip())
    if ',' in s and '.' in s:
        if s.rindex(',') > s.rindex('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def parse_nif(s):
    if not s:
        return ''
    m = re.search(r'\((\d{5,9})\)', str(s))
    return m.group(1) if m else ''

def parse_nome(s):
    if not s:
        return ''
    return re.sub(r'\s*\(\d+\)\s*$', '', str(s)).strip()

def parse_ano_de_data(v):
    if not v:
        return ''
    m = re.search(r'\b(20\d{2})\b', str(v))
    return m.group(1) if m else ''

def normalizar_data(v):
    if not v:
        return ''
    if hasattr(v, 'strftime'):
        return v.strftime('%Y-%m-%d')
    s = str(v).strip()
    m = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', s)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return s

# ─── Leitura XLSX ─────────────────────────────────────────────────────────────
def encontrar_coluna(header_row, nomes_possiveis):
    for nome in nomes_possiveis:
        for i, h in enumerate(header_row):
            if h and nome.lower().strip() in str(h).lower().strip():
                return i
    return None

def ler_xlsx(caminho_xlsx, ano=''):
    try:
        import openpyxl
    except ImportError:
        log.error("openpyxl não disponível.")
        return []

    log.info(f"  A ler: {Path(caminho_xlsx).name} …")
    try:
        wb = openpyxl.load_workbook(str(caminho_xlsx), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception as e:
        log.error(f"  Erro XLSX {caminho_xlsx}: {e}")
        return []

    if len(rows) < 2:
        return []

    # Encontrar linha de cabeçalho
    header_idx = 0
    for i, row in enumerate(rows[:5]):
        if row and any(cell and 'objeto' in str(cell).lower() for cell in row if cell):
            header_idx = i
            break

    header = [str(c).strip() if c else '' for c in rows[header_idx]]
    idx = {campo: encontrar_coluna(header, nomes) for campo, nomes in MAPA_COLUNAS.items()}
    log.info(f"  Colunas mapeadas: {[k for k, v in idx.items() if v is not None]}")

    def g(row, campo):
        i = idx.get(campo)
        if i is None or i >= len(row):
            return ''
        v = row[i]
        return str(v).strip() if v is not None else ''

    def gv(row, campo):
        i = idx.get(campo)
        if i is None or i >= len(row):
            return None
        return row[i]

    resultado = []
    for row in rows[header_idx + 1:]:
        if not row or not any(c for c in row if c is not None):
            continue
        adj_raw   = g(row, 'adj')
        adjt_raw  = g(row, 'adjt')
        tipo_proc = g(row, 'tipo_proc')
        local     = g(row, 'local')
        data_pub  = normalizar_data(gv(row, 'data_pub'))

        distrito = ''
        if local:
            partes = local.split(',')
            if len(partes) >= 2:
                distrito = partes[1].strip()

        d = {
            'objeto':        g(row, 'objeto'),
            'entidade_adj':  parse_nome(adj_raw),
            'adjudicataria': parse_nome(adjt_raw),
            'adj_nif':       parse_nif(adjt_raw),
            'tipo_contrato': g(row, 'tipo_cont'),
            'tipo_proc':     tipo_proc,
            'preco':         parse_preco(gv(row, 'preco')),
            'preco_efetivo': parse_preco(gv(row, 'preco_ef')),
            'data_pub':      data_pub,
            'data_contrato': normalizar_data(gv(row, 'data_cont')),
            'cpv':           g(row, 'cpv'),
            'distrito':      distrito,
            'ano':           ano or parse_ano_de_data(data_pub),
            'ajuste_direto': 1 if 'ajuste' in tipo_proc.lower() else 0,
            'fonte_ano':     ano,
        }
        if d['objeto'] or d['adjudicataria']:
            resultado.append(d)

    log.info(f"  → {len(resultado):,} contratos")
    return resultado

# ─── Motor de Risco ───────────────────────────────────────────────────────────
# Score 0-100 baseado em:
#  - World Bank Red Flags for Procurement
#  - Transparency International CRI
#  - OLAF (European Anti-Fraud Office) procurement indicators
#
# Limiares legais PT (Código dos Contratos Públicos, art. 20º):
#  Ajuste direto serviços/bens: até 20.000€ (simplificado) ou 75.000€ (regime geral)
#  Concurso público obrigatório: acima disso

PESOS = {
    'ajuste_simplificado':  25,
    'ajuste_direto':        18,
    'valor_acima_limiar':   15,
    'valor_muito_alto':      8,
    'desvio_extremo':       12,
    'desvio_moderado':       6,
    'sem_cpv':               5,
    'dados_incompletos':     8,
    'concentracao_alta':    15,
    'adj_repetida_alta':    15,
    'adj_repetida_media':    8,
}
LIMIAR_AJUSTE = 75_000
LIMIAR_ALTO   = 500_000

def calcular_score(c, hist=None):
    score = 0.0
    flags = []
    preco    = c.get('preco') or 0.0
    preco_ef = c.get('preco_efetivo') or 0.0
    proc     = (c.get('tipo_proc') or '').lower()
    cpv      = (c.get('cpv') or '').strip()

    if 'simplificado' in proc and 'ajuste' in proc:
        score += PESOS['ajuste_simplificado']
        flags.append('ajuste direto simplificado')
    elif 'ajuste' in proc:
        score += PESOS['ajuste_direto']
        flags.append('ajuste direto')

    if preco > LIMIAR_ALTO:
        score += PESOS['valor_acima_limiar'] + PESOS['valor_muito_alto']
        flags.append(f'valor muito elevado ({preco:,.0f}€)')
    elif preco > LIMIAR_AJUSTE:
        score += PESOS['valor_acima_limiar']
        flags.append(f'valor elevado ({preco:,.0f}€)')

    if preco > 0 and preco_ef > 0:
        delta = abs(preco_ef - preco) / preco
        if delta > 0.30:
            score += PESOS['desvio_extremo']
            flags.append(f'desvio preço {delta*100:.0f}%')
        elif delta > 0.15:
            score += PESOS['desvio_moderado']
            flags.append(f'desvio moderado {delta*100:.0f}%')

    if not cpv:
        score += PESOS['sem_cpv']
        flags.append('sem CPV')
    if not c.get('objeto') or not c.get('adjudicataria'):
        score += PESOS['dados_incompletos']
        flags.append('dados incompletos')

    if hist:
        n   = hist.get('n', 0)
        pct = hist.get('pct', 0.0)
        if n > 10:
            score += PESOS['adj_repetida_alta']
            flags.append(f'adjudicações repetidas ({n}x)')
        elif n > 5:
            score += PESOS['adj_repetida_media']
            flags.append(f'adj. repetida ({n}x)')
        if pct > 0.50:
            score += PESOS['concentracao_alta']
            flags.append(f'concentração {pct*100:.0f}% do valor')

    score = round(min(score, 100.0), 1)
    nivel = 'alto' if score >= 60 else ('medio' if score >= 35 else 'baixo')
    return score, flags, nivel

# ─── Download (sem requests — usa urllib stdlib) ───────────────────────────────
def download(url, destino, descricao=''):
    destino = Path(destino)
    if destino.exists() and destino.stat().st_size > 5000:
        log.info(f"  ✓ já existe: {destino.name} ({destino.stat().st_size/1e6:.1f} MB)")
        return True

    log.info(f"  ↓ {descricao or destino.name} …")
    req = Request(url, headers=HEADERS_HTTP)

    for t in range(1, 4):
        try:
            with urlopen(req, timeout=180) as resp:
                total  = int(resp.headers.get('Content-Length', 0))
                baixado = 0
                with open(destino, 'wb') as f:
                    while True:
                        chunk = resp.read(262144)  # 256KB
                        if not chunk:
                            break
                        f.write(chunk)
                        baixado += len(chunk)
                        if total:
                            print(f"\r     {baixado/total*100:5.1f}%  "
                                  f"{baixado/1e6:.1f}/{total/1e6:.1f} MB",
                                  end='', flush=True)
                print()
            log.info(f"  ✓ {destino.name} ({destino.stat().st_size/1e6:.1f} MB)")
            return True

        except HTTPError as e:
            log.warning(f"  HTTP {e.code} — tentativa {t}/3")
            if t < 3:
                time.sleep(3 * t)
        except URLError as e:
            log.warning(f"  Erro ligação: {e.reason} — tentativa {t}/3")
            if t < 3:
                time.sleep(5 * t)
        except Exception as e:
            log.error(f"  Erro: {e}")
            break

    if destino.exists():
        destino.unlink()
    return False

def extrair_zip(caminho_zip, pasta_destino):
    resultado = []
    try:
        with zipfile.ZipFile(caminho_zip, 'r') as z:
            for nome in z.namelist():
                if nome.lower().endswith(('.xlsx', '.csv')):
                    dest = pasta_destino / Path(nome).name
                    if not dest.exists():
                        z.extract(nome, pasta_destino)
                        extraido = pasta_destino / nome
                        if extraido != dest and extraido.exists():
                            extraido.rename(dest)
                    resultado.append(dest)
        log.info(f"  Extraído: {[f.name for f in resultado]}")
    except Exception as e:
        log.error(f"  Erro a extrair ZIP: {e}")
    return resultado

# ─── BD ───────────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def upsert(conn, c, score, flags, nivel):
    agora = datetime.now().isoformat()
    conn.execute("""
        INSERT OR IGNORE INTO contratos
          (objeto, entidade_adj, adjudicataria, adj_nif,
           tipo_contrato, tipo_proc, preco, preco_efetivo,
           data_pub, data_contrato, cpv, distrito, ano,
           ajuste_direto, score_risco, nivel_risco, flags, fonte_ano, atualizado)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        c.get('objeto'),       c.get('entidade_adj'),   c.get('adjudicataria'),
        c.get('adj_nif'),      c.get('tipo_contrato'),  c.get('tipo_proc'),
        c.get('preco'),        c.get('preco_efetivo'),
        c.get('data_pub'),     c.get('data_contrato'),
        c.get('cpv'),          c.get('distrito'),        c.get('ano'),
        c.get('ajuste_direto', 0),
        score, nivel, json.dumps(flags, ensure_ascii=False),
        c.get('fonte_ano', ''), agora,
    ))

def agregar(conn):
    agora = datetime.now().isoformat()
    conn.execute("DELETE FROM entidades")
    conn.execute("""
        INSERT INTO entidades (nome, nif, total_contratos, total_valor,
                               score_avg, score_max, pct_ajuste, atualizado)
        SELECT adjudicataria, adj_nif,
               COUNT(*),
               SUM(COALESCE(preco,0)),
               ROUND(AVG(score_risco),1),
               ROUND(MAX(score_risco),1),
               ROUND(CAST(SUM(ajuste_direto) AS REAL)/COUNT(*),4),
               ?
        FROM contratos
        WHERE adjudicataria IS NOT NULL AND adjudicataria != ''
        GROUP BY adjudicataria
    """, (agora,))
    conn.execute("DELETE FROM adjudicantes")
    conn.execute("""
        INSERT INTO adjudicantes (nome, total_contratos, total_valor,
                                  score_avg, pct_ajuste, atualizado)
        SELECT entidade_adj,
               COUNT(*),
               SUM(COALESCE(preco,0)),
               ROUND(AVG(score_risco),1),
               ROUND(CAST(SUM(ajuste_direto) AS REAL)/COUNT(*),4),
               ?
        FROM contratos
        WHERE entidade_adj IS NOT NULL AND entidade_adj != ''
        GROUP BY entidade_adj
    """, (agora,))
    conn.commit()

# ─── Exportações ──────────────────────────────────────────────────────────────
def exportar_json(conn):
    path = PASTA / "export_completo.json"
    contratos    = [dict(r) for r in conn.execute(
        "SELECT * FROM contratos ORDER BY score_risco DESC LIMIT 15000")]
    entidades    = [dict(r) for r in conn.execute(
        "SELECT * FROM entidades ORDER BY score_avg DESC LIMIT 1000")]
    adjudicantes = [dict(r) for r in conn.execute(
        "SELECT * FROM adjudicantes ORDER BY score_avg DESC LIMIT 500")]
    stats = dict(conn.execute("""
        SELECT COUNT(*) total,
          SUM(CASE WHEN nivel_risco='alto'  THEN 1 ELSE 0 END) alto,
          SUM(CASE WHEN nivel_risco='medio' THEN 1 ELSE 0 END) medio,
          SUM(CASE WHEN nivel_risco='baixo' THEN 1 ELSE 0 END) baixo,
          SUM(COALESCE(preco,0)) valor_total,
          SUM(CASE WHEN ajuste_direto=1 THEN COALESCE(preco,0) ELSE 0 END) valor_ajuste,
          ROUND(AVG(score_risco),2) score_medio,
          SUM(ajuste_direto) total_ajuste
        FROM contratos
    """).fetchone())
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({
            'gerado':       datetime.now().isoformat(),
            'fonte':        'dados.gov.pt — IMPIC/Portal Base (domínio público)',
            'estatisticas': stats,
            'entidades':    entidades,
            'adjudicantes': adjudicantes,
            'contratos':    contratos,
        }, f, ensure_ascii=False, indent=2)
    log.info(f"JSON → {path} ({len(contratos):,} contratos)")

def exportar_csv_alto_risco(conn):
    path = PASTA / "alto_risco.csv"
    rows = conn.execute(
        "SELECT * FROM contratos WHERE nivel_risco='alto' ORDER BY score_risco DESC"
    ).fetchall()
    if not rows:
        log.warning("Sem contratos de alto risco para exportar.")
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows([dict(r) for r in rows])
    log.info(f"CSV alto risco → {path} ({len(rows):,} linhas)")

def exportar_csv_completo(conn):
    path = PASTA_PROC / "contratos_todos.csv"
    rows = conn.execute("SELECT * FROM contratos ORDER BY score_risco DESC").fetchall()
    if not rows:
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows([dict(r) for r in rows])
    log.info(f"CSV completo → {path} ({len(rows):,} linhas)")

def gerar_relatorio(conn):
    path = PASTA / "relatorio_resumo.txt"
    L = []
    def p(s=''): L.append(s)

    s = dict(conn.execute("""
        SELECT COUNT(*) total,
          SUM(CASE WHEN nivel_risco='alto'  THEN 1 ELSE 0 END) alto,
          SUM(CASE WHEN nivel_risco='medio' THEN 1 ELSE 0 END) medio,
          SUM(CASE WHEN nivel_risco='baixo' THEN 1 ELSE 0 END) baixo,
          SUM(COALESCE(preco,0)) valor_total,
          SUM(CASE WHEN ajuste_direto=1 THEN COALESCE(preco,0) ELSE 0 END) valor_ajuste,
          ROUND(AVG(score_risco),1) score_medio,
          SUM(ajuste_direto) total_ajuste
        FROM contratos
    """).fetchone())
    for key in ['total', 'alto', 'medio', 'baixo', 'valor_total', 'valor_ajuste', 'score_medio', 'total_ajuste']:
        s[key] = s.get(key) or 0

    p('═'*70)
    p('PT-VIGIL — RELATÓRIO DE RISCO NOS CONTRATOS PÚBLICOS PORTUGUESES')
    p(f'Gerado : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    p(f'Fonte  : dados.gov.pt — IMPIC / Portal Base (domínio público)')
    p('═'*70)
    p()
    p('RESUMO GLOBAL')
    p('─'*50)
    p(f'  Total de contratos    : {s["total"]:>12,}')
    p(f'  Valor total           : {(s["valor_total"] or 0)/1e9:>10.2f} mil M€')
    p(f'  Score médio de risco  : {(s["score_medio"] or 0):>10.1f} / 100')
    p(f'  ⚠  Alto risco (≥60)   : {s["alto"]:>12,} contratos')
    p(f'  ⚡ Risco médio (35-59) : {s["medio"]:>12,} contratos')
    p(f'  ✓  Baixo risco (<35)  : {s["baixo"]:>12,} contratos')
    p(f'  Por ajuste direto     : {s["total_ajuste"]:>12,}'
      f'  ({(s["valor_ajuste"] or 0)/1e9:.2f} mil M€)')
    p()

    anos = conn.execute("""
        SELECT ano, COUNT(*) n, SUM(COALESCE(preco,0)) v, AVG(score_risco) sc
        FROM contratos WHERE ano != ''
        GROUP BY ano ORDER BY ano DESC
    """).fetchall()
    if anos:
        p('DISTRIBUIÇÃO POR ANO')
        p('─'*50)
        for a in anos:
            p(f'  {a["ano"]}  {a["n"]:7,} contratos  '
              f'{(a["v"] or 0)/1e9:7.2f} mil M€  score médio {(a["sc"] or 0):.1f}')
        p()

    alto = conn.execute(
        "SELECT * FROM contratos WHERE nivel_risco='alto' ORDER BY score_risco DESC LIMIT 25"
    ).fetchall()
    p(f'TOP 25 CONTRATOS ALTO RISCO')
    p('─'*70)
    for c in alto:
        fl = json.loads(c['flags'] or '[]')
        p(f"  [{c['score_risco']:5.1f}] {(c['objeto'] or '?')[:62]}")
        p(f"         ▸ {(c['entidade_adj'] or '?')[:35]} → {(c['adjudicataria'] or '?')[:35]}")
        p(f"         ▸ {(c['preco'] or 0):>12,.0f}€  |  {c['ano'] or '?'}"
          f"  |  {', '.join(fl)}")
        p()

    top_ent = conn.execute(
        "SELECT * FROM entidades ORDER BY score_avg DESC LIMIT 20"
    ).fetchall()
    p('TOP 20 ADJUDICATÁRIAS POR RISCO MÉDIO')
    p('─'*70)
    for e in top_ent:
        p(f"  [{e['score_avg']:5.1f}]  {(e['nome'] or '?')[:55]}")
        p(f"           {e['total_contratos']:,} contratos  |  "
          f"{(e['total_valor'] or 0)/1e6:.1f} M€  |  "
          f"{(e['pct_ajuste'] or 0)*100:.0f}% ajuste direto")
        p()

    top_pub = conn.execute(
        "SELECT * FROM adjudicantes WHERE total_contratos >= 5 "
        "ORDER BY pct_ajuste DESC LIMIT 15"
    ).fetchall()
    p('TOP 15 ENTIDADES PÚBLICAS — MAIOR % AJUSTE DIRETO (≥5 contratos)')
    p('─'*70)
    for e in top_pub:
        p(f"  {(e['pct_ajuste'] or 0)*100:5.1f}%  {(e['nome'] or '?')[:55]}")
        p(f"         {e['total_contratos']} contratos  |  "
          f"{(e['total_valor'] or 0)/1e6:.1f} M€  |  score {e['score_avg']:.1f}")
        p()

    p('═'*70)
    p('FIM DO RELATÓRIO — PT-VIGIL v3')
    p('═'*70)

    texto = '\n'.join(L)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(texto)
    log.info(f"Relatório → {path}")
    return texto

# ─── PIPELINE ─────────────────────────────────────────────────────────────────
def fase1_descarga(anos):
    PASTA_ZIP.mkdir(parents=True, exist_ok=True)
    PASTA_XLSX.mkdir(parents=True, exist_ok=True)

    log.info(f"FASE 1 — DESCARGA: {anos}")
    log.info("Fonte: dados.gov.pt — IMPIC/Portal Base (domínio público, licença livre)")

    ficheiros = []
    for ano in anos:
        if ano not in FICHEIROS:
            log.warning(f"  Ano {ano} não disponível. Anos válidos: {ANOS_TODOS}")
            continue

        url, tipo = FICHEIROS[ano]

        if tipo == 'zip':
            dest = PASTA_ZIP / f"contratos{ano}.zip"
            ok   = download(url, dest, f"contratos{ano}.zip")
            if not ok and ano in PERMALINKS:
                ok = download(PERMALINKS[ano], dest, f"contratos{ano}.zip (permalink)")
            if ok:
                xlsx_list = extrair_zip(dest, PASTA_XLSX)
                ficheiros.extend([(f, ano) for f in xlsx_list])
        else:
            dest = PASTA_XLSX / f"contratos{ano}.xlsx"
            ok   = download(url, dest, f"contratos{ano}.xlsx")
            if not ok and ano in PERMALINKS:
                ok = download(PERMALINKS[ano], dest, f"contratos{ano}.xlsx (permalink)")
            if ok:
                ficheiros.append((dest, ano))

    log.info(f"Fase 1 completa — {len(ficheiros)} ficheiros")
    return ficheiros

def fase2_processamento(ficheiros, conn):
    log.info(f"FASE 2 — PROCESSAMENTO: {len(ficheiros)} ficheiros")

    todos = []
    for caminho, ano in ficheiros:
        lote = ler_xlsx(caminho, ano)
        todos.extend(lote)

    if not todos:
        log.error("Nenhum contrato lido. Verifica os ficheiros em pt_vigil_dados/xlsx/")
        return 0, 0

    log.info(f"Total bruto: {len(todos):,} contratos")

    # Concentração global
    cnt_adj = {}
    val_adj = {}
    for c in todos:
        adj = (c.get('adjudicataria') or '').strip()
        if adj:
            cnt_adj[adj] = cnt_adj.get(adj, 0) + 1
            val_adj[adj] = val_adj.get(adj, 0) + (c.get('preco') or 0)
    val_total = sum(val_adj.values()) or 1

    total_proc = 0
    alto_risco = 0
    conn.execute("BEGIN")

    for i, c in enumerate(todos):
        adj  = (c.get('adjudicataria') or '').strip()
        hist = {'n': cnt_adj.get(adj, 0), 'pct': val_adj.get(adj, 0) / val_total}
        score, flags, nivel = calcular_score(c, hist)
        upsert(conn, c, score, flags, nivel)
        total_proc += 1
        if nivel == 'alto':
            alto_risco += 1
        if i % 10000 == 0 and i > 0:
            conn.commit()
            conn.execute("BEGIN")
            log.info(f"  … {i:,} / {len(todos):,}")

    conn.commit()
    log.info(f"Fase 2 — {total_proc:,} processados, {alto_risco:,} alto risco")
    return total_proc, alto_risco

def fase3_exportacao(conn):
    log.info("FASE 3 — EXPORTAÇÃO")
    PASTA_PROC.mkdir(exist_ok=True)
    agregar(conn)
    exportar_json(conn)
    exportar_csv_alto_risco(conn)
    exportar_csv_completo(conn)
    texto = gerar_relatorio(conn)
    print()
    print(texto)

def mostrar_estrutura():
    print()
    print("╔══════════════════════════════════════════╗")
    print("║   PT-VIGIL — FICHEIROS CRIADOS           ║")
    print("╚══════════════════════════════════════════╝")
    for f in sorted(PASTA.rglob("*")):
        if f.is_file():
            sz = f.stat().st_size
            sz_s = f"{sz/1e6:.1f} MB" if sz > 1e6 else f"{sz/1e3:.0f} KB"
            profundidade = len(f.relative_to(PASTA).parts) - 1
            indent = "  " + "│  " * profundidade + "├─ "
            print(f"{indent}{f.name}  ({sz_s})")
    print()
    print("📤 PARA O DASHBOARD:")
    print(f"   export_completo.json  → carrega no pt-vigil-real.html")
    print(f"   alto_risco.csv        → abre no LibreOffice Calc")
    print(f"   relatorio_resumo.txt  → lê directamente")
    print(f"   pt_vigil.db           → SQLite para queries avançadas")
    print()

# ─── CLI ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='PT-VIGIL v3 — Contratos públicos via dados.gov.pt (IMPIC)'
    )
    parser.add_argument(
        '--anos', nargs='*', default=None,
        help='Anos (ex: --anos 2023 2024 2025) ou "todos" para 2012-2026. '
             f'Default: {ANOS_RECENTES}'
    )
    parser.add_argument('--so-processar', action='store_true',
                        help='Não descarregar — processar XLSX já existentes')
    parser.add_argument('--limpar', action='store_true',
                        help='Apagar BD antes de começar (mantém ZIPs/XLSXs)')
    args = parser.parse_args()

    # Resolver anos
    if args.anos is None:
        anos = ANOS_RECENTES
    elif 'todos' in args.anos:
        anos = ANOS_TODOS
    else:
        anos = args.anos

    # Setup
    PASTA.mkdir(exist_ok=True)
    setup_logging()

    if not garantir_openpyxl():
        sys.exit(1)

    if args.limpar and DB_PATH.exists():
        log.warning("--limpar: a apagar BD anterior …")
        DB_PATH.unlink()

    log.info("=" * 60)
    log.info("PT-VIGIL PIPELINE v3 — INICIADO")
    log.info(f"Anos: {anos}")
    log.info("=" * 60)
    inicio = time.time()

    # Fase 1
    if args.so_processar:
        PASTA_XLSX.mkdir(parents=True, exist_ok=True)
        ficheiros = [(f, f.stem.replace('contratos', ''))
                     for f in PASTA_XLSX.glob("*.xlsx")]
        log.info(f"Modo --so-processar: {len(ficheiros)} XLSX encontrados")
    else:
        ficheiros = fase1_descarga(anos)

    # Fase 2
    conn = init_db()
    total, alto = fase2_processamento(ficheiros, conn)

    # Fase 3
    fase3_exportacao(conn)
    conn.close()

    duracao = time.time() - inicio
    log.info(f"Pipeline concluído em {duracao:.0f}s ({duracao/60:.1f} min)")
    mostrar_estrutura()

if __name__ == '__main__':
    main()
