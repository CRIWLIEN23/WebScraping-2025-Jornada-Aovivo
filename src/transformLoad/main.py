# =============================
# main.py (versão corrigida e robusta)
# =============================

import pandas as pd
import sqlite3
import json
from datetime import datetime
from pathlib import Path          # <-- estava faltando
from io import StringIO           # <-- estava faltando
from typing import Union          # opcional, só pra deixar bonitinho


def load_jsonl_safe(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Carrega um arquivo .jsonl de forma segura:
    - verifica se o arquivo existe
    - verifica se está vazio
    - ignora linhas corrompidas e avisa quais são
    - evita o FutureWarning do pandas
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    if file_path.stat().st_size == 0:
        print("Aviso: O arquivo data.jsonl está vazio! Execute o scraper primeiro.")
        return pd.DataFrame()

    valid_lines = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)           # só valida
                valid_lines.append(line)
            except json.JSONDecodeError as e:
                print(f"Linha {line_num} inválida (será ignorada): {e}")
                print(f"    Conteúdo: {line[:120]}...")

    if not valid_lines:
        print("Nenhuma linha válida encontrada no arquivo!")
        return pd.DataFrame()

    print(f"Carregadas {len(valid_lines)} linhas válidas de {file_path.name}")
    return pd.read_json(StringIO("\n".join(valid_lines)), lines=True)


# =============================
# EXECUÇÃO PRINCIPAL
# =============================

# 1. Carregar os dados
df = load_jsonl_safe('../../data/data.jsonl')

if df.empty:
    print("Nada para processar. Encerrando.")
    exit()

print(f"Total de produtos carregados: {len(df)}")

# 2. Adicionar colunas fixas
df['_source'] = "https://lista.mercadolivre.com.br/notebook"
df['_datetime'] = datetime.now()

# 3. Tratar valores nulos
df['old_money'] = df['old_money'].fillna('0')
df['new_money'] = df['new_money'].fillna('0')
df['reviews_rating_number'] = df['reviews_rating_number'].fillna('0')
df['reviews_amount'] = df['reviews_amount'].fillna('(0)')

# 4. Limpar formatação brasileira (pontos, parênteses, R$ etc.)
df['old_money'] = df['old_money'].astype(str).str.replace(r'\.', '', regex=True).str.replace(r'R\$', '', regex=True).str.strip()
df['new_money'] = df['new_money'].astype(str).str.replace(r'\.', '', regex=True).str.replace(r'R\$', '', regex=True).str.strip()
df['reviews_amount'] = df['reviews_amount'].astype(str).str.replace(r'[()\.]', '', regex=True)

# 5. Converter para números (de forma segura)
df['old_money'] = pd.to_numeric(df['old_money'], errors='coerce').fillna(0)
df['new_money'] = pd.to_numeric(df['new_money'], errors='coerce').fillna(0)
df['reviews_rating_number'] = pd.to_numeric(df['reviews_rating_number'], errors='coerce').fillna(0)
df['reviews_amount'] = pd.to_numeric(df['reviews_amount'], errors='coerce').fillna(0).astype(int)

# 6. Filtrar faixa de preço desejada
df = df[
    df['old_money'].between(1000, 10000) &
    df['new_money'].between(1000, 10000)
].copy()

print(f"Após filtro de preço (R$ 1.000 - R$ 10.000): {len(df)} produtos")

# 7. Salvar no SQLite
db_path = Path('data/mercadolivre.db')
db_path.parent.mkdir(parents=True, exist_ok=True)  # cria a pasta se não existir

conn = sqlite3.connect(db_path)
df.to_sql('notebook', conn, if_exists='replace', index=False)
conn.close()

print(f"Dados salvos com sucesso em {db_path}")
print("Processamento concluído!")