import concurrent.futures
import json
import re
import time
from datetime import datetime
from functools import partial

import pandas as pd
import requests

# --- CONFIGURAÇÕES ---
# O controle será feito pelo número de threads
PAUSA_ENTRE_REQUISICOES_SEG = 0.2 
ITENS_POR_PAGINA_LICITACOES = 50
MAXIMO_PAGINAS_POR_MODALIDADE = 2
# <<< Número máximo de requisições paralelas >>>
MAX_WORKERS_THREADS = 10 

# --- URLs DAS APIs ---
URL_API_PNCP_CONSULTA_BASE = "https://pncp.gov.br/api/consulta"
ENDPOINT_PNCP_BUSCA_LICITACOES = "/v1/contratacoes/publicacao"
URL_API_PNCP_INTEGRACAO_BASE = "https://pncp.gov.br/api/pncp"

# --- Códigos de Modalidade ---
MODALIDADES = list(range(1, 14)) # 1 a 13

# --- FUNÇÃO 1: BUSCAR LICITAÇÕES (REFEITA PARA PARALELISMO) ---

def _fetch_pagina_modalidade(params_base, cod_modalidade, pagina_atual):
    """Função auxiliar que busca UMA PÁGINA de UMA MODALIDADE."""
    print(f"  Buscando Modalidade: {cod_modalidade}, Página: {pagina_atual}...")
    
    url_busca = f"{URL_API_PNCP_CONSULTA_BASE}{ENDPOINT_PNCP_BUSCA_LICITACOES}"
    headers = {'Accept': 'application/json'}
    
    params = params_base.copy()
    params["codigoModalidadeContratacao"] = cod_modalidade
    params["pagina"] = pagina_atual
    
    try:
        # A pausa é pequena, pois o ThreadPool já limita o número de requisições
        time.sleep(PAUSA_ENTRE_REQUISICOES_SEG) 
        response = requests.get(url_busca, headers=headers, params=params, timeout=60)

        if response.status_code == 200:
            dados = response.json()
            licitacoes_pagina = dados.get('data', [])
            
            licitacoes_processadas = []
            for lic in licitacoes_pagina:
                licitacoes_processadas.append({
                    'id_pncp': lic.get('numeroControlePNCP'),
                    'ano': lic.get('anoCompra'),
                    'sequencial': lic.get('sequencialCompra'),
                    'modalidade_nome': lic.get('modalidadeNome'),
                    'objeto': lic.get('objetoCompra', ''),
                    'valor_total_estimado_licitacao': lic.get('valorTotalEstimado'),
                    'data_publicacao': lic.get('dataPublicacaoPNCP')
                })
            
            # Retorna as licitações E se deve parar de buscar esta modalidade
            parar_busca = (pagina_atual >= dados.get('totalPaginas', 1)) or (not licitacoes_pagina)
            return licitacoes_processadas, parar_busca, cod_modalidade
        
        elif response.status_code == 204:
            print(f"  Nenhuma licitação encontrada para Modalidade {cod_modalidade}.")
            return [], True, cod_modalidade # Parar busca
        else:
            print(f"  Erro ao buscar Modalidade {cod_modalidade} (Pág {pagina_atual}): Status {response.status_code}")
            return [], True, cod_modalidade # Parar busca

    except Exception as e:
        print(f"  Erro de conexão/timeout (Modalidade {cod_modalidade}, Pág {pagina_atual}): {e}")
        return [], True, cod_modalidade # Parar busca


def buscar_licitacoes_recentes(cnpj, data_inicial_str, data_final_str):
    """Busca licitações publicadas no PNCP iterando por todas as modalidades EM PARALELO."""
    print(f"\n--- Buscando licitações para {cnpj} (EM PARALELO) ---")
    
    params_base = {
        "dataInicial": data_inicial_str,
        "dataFinal": data_final_str,
        "cnpj": cnpj,
        "tamanhoPagina": ITENS_POR_PAGINA_LICITACOES
    }

    licitacoes_encontradas_total = []
    # Cria uma lista de "tarefas" para a primeira página de cada modalidade
    tarefas = []
    for cod_modalidade in MODALIDADES:
        tarefas.append((cod_modalidade, 1)) # (modalidade, pagina)

    modalidades_ativas = set(MODALIDADES)

    # Usa o ThreadPoolExecutor para rodar as buscas em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_THREADS) as executor:
        
        # Prepara a função a ser chamada, fixando o argumento 'params_base'
        func_partial = partial(_fetch_pagina_modalidade, params_base)
        
        # Mapeia as tarefas para os threads. 
        # 'futures' é uma lista de tarefas sendo executadas
        futures = {executor.submit(func_partial, cod, pag): (cod, pag) for (cod, pag) in tarefas}

        while futures:
            # Espera a próxima tarefa ser concluída
            done_future = next(concurrent.futures.as_completed(futures))
            tarefa_original = futures.pop(done_future) # Remove da lista de ativas
            
            try:
                licitacoes, parar_busca, cod_modalidade = done_future.result()
                
                if licitacoes:
                    licitacoes_encontradas_total.extend(licitacoes)
                
                # Se não devemos parar e não atingimos o limite de páginas
                pagina_atual = tarefa_original[1]
                if not parar_busca and (pagina_atual < MAXIMO_PAGINAS_POR_MODALIDADE) and (cod_modalidade in modalidades_ativas):
                    # Adiciona a próxima página desta modalidade na fila de tarefas
                    proxima_pagina = pagina_atual + 1
                    print(f"  Modalidade {cod_modalidade}: Adicionando página {proxima_pagina} à fila.")
                    nova_tarefa = (cod_modalidade, proxima_pagina)
                    futures[executor.submit(func_partial, *nova_tarefa)] = nova_tarefa
                elif cod_modalidade in modalidades_ativas:
                    # Remove a modalidade da lista ativa pois ela terminou
                    print(f"  Modalidade {cod_modalidade}: Fim dos resultados.")
                    modalidades_ativas.remove(cod_modalidade)

            except Exception as e:
                print(f"  Erro ao processar resultado da tarefa {tarefa_original}: {e}")

    print(f"\n--- Total de {len(licitacoes_encontradas_total)} licitações encontradas. ---")
    return licitacoes_encontradas_total


# --- FUNÇÃO 2: BUSCAR ITENS (Modificada para ser chamada em paralelo) ---

def buscar_itens_licitacao(cnpj, ano, sequencial):
    """Busca os itens de UMA licitação específica."""
    # Esta função será chamada em paralelo, então o print é importante
    print(f"  Buscando itens para licitação {ano}/{sequencial}...")
    itens_encontrados = []
    url_itens = f"{URL_API_PNCP_INTEGRACAO_BASE}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    headers = {'Accept': 'application/json'}

    try:
        # A pausa é pequena, pois o ThreadPool já limita o número de requisições
        time.sleep(PAUSA_ENTRE_REQUISICOES_SEG) 
        response = requests.get(url_itens, headers=headers, timeout=20)

        if response.status_code == 200:
            itens_bruto = response.json()
            for item in itens_bruto:
                itens_encontrados.append({
                    'numero_item': item.get('numeroItem'),
                    'tipo': item.get('materialOuServicoNome'),
                    'descricao': item.get('descricao', '').strip(),
                    'quantidade': item.get('quantidade'),
                    'valor_unit_estimado': item.get('valorUnitarioEstimado'),
                    'valor_total_estimado': item.get('valorTotalEstimado')
                })
        else:
            print(f"  Erro ao buscar itens para {ano}/{sequencial}: Status {response.status_code}")
        
    except Exception as e:
        print(f"  Erro de conexão ao buscar itens para {ano}/{sequencial}: {e}")

    return itens_encontrados


# --- FUNÇÃO 3: FUNÇÃO "MESTRA" ---

def _fetch_e_enriquece_itens(licitacao, cnpj):
    """Função auxiliar que busca itens E já enriquece com dados da licitação."""
    
    # Pega os dados que precisamos para a URL
    ano = licitacao.get('ano')
    sequencial = licitacao.get('sequencial')

    if not ano or not sequencial:
        return []
        
    itens = buscar_itens_licitacao(cnpj, ano, sequencial)
    
    itens_enriquecidos = []
    for item in itens:
        # Adicionamos os componentes da URL individualmente
        item['cnpj'] = cnpj
        item['ano'] = ano
        item['sequencial'] = sequencial
        
        item['id_pncp'] = licitacao.get('id_pncp') 
        item['licitacao_id'] = f"{ano}/{sequencial}"
        item['licitacao_objeto'] = licitacao['objeto']
        item['licitacao_modalidade'] = licitacao['modalidade_nome']
        item['licitacao_data_publicacao'] = licitacao['data_publicacao']
        itens_enriquecidos.append(item)
    return itens_enriquecidos


def gerar_relatorio_bruto(cnpj, data_inicio_str, data_fim_str):
    """
    Função principal que orquestra a busca de licitações e seus itens,
    agora usando paralelismo para ambas as etapas.
    """
    
    try:
        d_inicio = datetime.strptime(data_inicio_str, '%Y%m%d')
        d_fim = datetime.strptime(data_fim_str, '%Y%m%d')
        if (d_fim - d_inicio).days > 366:
            print("Erro: Período excede 1 ano.")
            return []
    except ValueError:
        print("Erro: Formato de data inválido. Use YYYYMMDD.")
        return []

    # --- Etapa 1: Buscar licitações (Já está em paralelo) ---
    licitacoes = buscar_licitacoes_recentes(cnpj, data_inicio_str, data_fim_str)
    
    if not licitacoes:
        print("Nenhuma licitação encontrada.")
        return []

    print(f"\n--- Processando {len(licitacoes)} licitações para buscar itens (EM PARALELO) ---")
    
    todos_os_itens = []
    
    # --- Etapa 2: Buscar itens (EM PARALELO) ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_THREADS) as executor:
        # Prepara a função a ser chamada, fixando o argumento 'cnpj'
        func_partial = partial(_fetch_e_enriquece_itens, cnpj=cnpj)
        
        # 'map' aplica a função 'func_partial' a cada item da lista 'licitacoes'
        # e retorna os resultados na ordem
        resultados_listas_de_itens = executor.map(func_partial, licitacoes)
        
        # 'resultados' é uma lista de listas (ex: [[item1, item2], [item3], []])
        for lista_de_itens in resultados_listas_de_itens:
            todos_os_itens.extend(lista_de_itens)

    print(f"\n--- Relatório Concluído: {len(todos_os_itens)} itens encontrados ---")
    return todos_os_itens

# --- Exemplo de uso dessa nova função ---
if __name__ == "__main__":
    
    CNPJ_CIDADE = "13825484000150" # CNPJ de Amargosa
    DATA_INICIO = "20251001"
    DATA_FIM = "20251021"

    # Medindo o tempo
    start_time = time.time()
    
    itens_do_relatorio = gerar_relatorio_bruto(CNPJ_CIDADE, DATA_INICIO, DATA_FIM)
    
    
    end_time = time.time()
    print(f"\nTempo total da busca: {end_time - start_time:.2f} segundos")

    if itens_do_relatorio:
        df = pd.DataFrame(itens_do_relatorio)
        colunas = [
            'licitacao_id', 'licitacao_data_publicacao', 'licitacao_modalidade', 
            'numero_item', 'descricao', 'quantidade', 'valor_unit_estimado', 
            'valor_total_estimado', 'tipo', 'licitacao_objeto', 'id_pncp'
        ]
        colunas_finais = [col for col in colunas if col in df.columns]
        df = df[colunas_finais]

        print(df.head())
        
        df.to_csv("relatorio_bruto_colaborativo.csv", index=False, sep=';', decimal=',', encoding='utf-8-sig')
        print("\nRelatório bruto salvo em 'relatorio_bruto_colaborativo.csv'")