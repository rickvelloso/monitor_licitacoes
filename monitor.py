import json
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import spacy  # Importa a biblioteca de IA (NLP)
from bs4 import BeautifulSoup

# --- CONFIGURAÇÕES ---
CNPJ_AMARGOSA = "13825484000150"
DIAS_PARA_BUSCAR = 30 # Buscar licitações dos últimos 30 dias
LIMITE_ALERTA_PERCENTUAL = 0.30 # 30% acima do preço de referência
ITENS_POR_PAGINA_LICITACOES = 50
MAXIMO_PAGINAS_POR_MODALIDADE = 2 # Limite para não sobrecarregar (máx 2 páginas por modalidade)
PAUSA_ENTRE_REQUISICOES_SEG = 1

# --- URLs DAS APIs ---
URL_API_PNCP_CONSULTA_BASE = "https://pncp.gov.br/api/consulta"
ENDPOINT_PNCP_BUSCA_LICITACOES = "/v1/contratacoes/publicacao"
URL_API_PNCP_INTEGRACAO_BASE = "https://pncp.gov.br/api/pncp"

# --- Códigos de Modalidade (Manual Tabela 5.2) ---
MODALIDADES = list(range(1, 14)) # De 1 a 13

# --- INICIALIZAÇÃO DA IA (NLP) ---
print("Carregando modelo de IA (spaCy)...")
try:
    # Carrega o modelo de português pequeno. 
    # Isso será usado para normalizar e entender os nomes dos itens.
    NLP_MODEL = spacy.load("pt_core_news_sm")
    print("Modelo de IA carregado com sucesso.")
except IOError:
    print("\n[ERRO] Modelo 'pt_core_news_sm' não encontrado.")
    print("Por favor, execute: python -m spacy download pt_core_news_sm")
    exit()


# --- Palavras-chave de licitação para remover ---
# Esta lista ajuda a IA a limpar o nome do item para a BUSCA DE PREÇO
PALAVRAS_REMOVER_LICITACAO = {
    # Termos Gerais de Licitação
    'AQUISIÇÃO', 'AQUISICAO', 'CONTRATAÇÃO', 'CONTRATACAO', 'COMPRA', 'REGISTRO', 
    'PREÇO', 'PRECO', 'FORNECIMENTO', 'FUTURA', 'EVENTUAL', 'MATERIAL', 'SERVIÇO', 
    'SERVICO', 'ITEM', 'UNIDADE', 'UN', 'UND', 'MARCA', 'MODELO', 'TIPO', 
    'REFERÊNCIA', 'REFERENCIA', 'REF', 'DESCRIÇÃO', 'DESCRICAO', 'CONFORME', 
    'ESPECIFICAÇÕES', 'ESPECIFICACOES', 'TÉCNICAS', 'TECNICAS', 'ANEXO', 'EDITAL', 
    'TERMO', 'PROCESSO', 'LICITACAO', 'LICITAÇÃO', 'OBJETO', 'LOTE',
    
    # Palavras de "kit" (são removidas da busca, mas detectadas pela heurística)
    'KIT', 'CONJUNTO', 'CAIXA', 'PACOTE', 'FARDO', 'JG', 'JOGO', 'CX', 'PCT',
    
    # NOVAS PALAVRAS (do Log) para limpar a busca
    'CARACTERÍSTICAS', 'CARACTERISTICAS', 'ADICIONAIS', 'ADICIONAL',
    'APLICAÇÃO', 'APLICACAO', 'ATIVIDADES', 'DIVERSAS',
    'BENEFICIADA', 'APRESENTAÇÃO', 'APRESENTACAO', 'FARELO', 'CLASSE',
    'ASPECTO', 'FÍSICO', 'FISICO', 'PRAZO', 'VALIDADE', 'MÍNIMO', 'MINIMO',
    'BASE', 'MASSA', 'SEGURANÇA', 'SEGURANCA', 'REDE', 'COMPUTADORES'
}


# --- FUNÇÕES AUXILIARES ---

def buscar_licitacoes_recentes(cnpj, dias_atras):
    """Busca licitações publicadas no PNCP iterando por todas as modalidades."""
    print(f"\n--- Buscando licitações para {cnpj} (últimos {dias_atras} dias) ---")
    licitacoes_encontradas_total = []

    data_hoje = datetime.now()
    data_inicio = data_hoje - timedelta(days=dias_atras)
    formato_data = "%Y%m%d"
    data_final_str = data_hoje.strftime(formato_data)
    data_inicial_str = data_inicio.strftime(formato_data)

    url_busca = f"{URL_API_PNCP_CONSULTA_BASE}{ENDPOINT_PNCP_BUSCA_LICITACOES}"
    headers = {'Accept': 'application/json'}

    for cod_modalidade in MODALIDADES:
        print(f"\nBuscando Modalidade: {cod_modalidade}...")
        licitacoes_modalidade = []

        for pagina_atual in range(1, MAXIMO_PAGINAS_POR_MODALIDADE + 1):
            print(f"  Buscando página {pagina_atual}...")
            params = {
                "dataInicial": data_inicial_str,
                "dataFinal": data_final_str,
                "codigoModalidadeContratacao": cod_modalidade,
                "cnpj": cnpj,
                "pagina": pagina_atual,
                "tamanhoPagina": ITENS_POR_PAGINA_LICITACOES
            }

            try:
                time.sleep(PAUSA_ENTRE_REQUISICOES_SEG)
                response = requests.get(url_busca, headers=headers, params=params, timeout=60)

                if response.status_code == 200:
                    dados = response.json()
                    licitacoes_pagina = dados.get('data', [])
                    if not licitacoes_pagina:
                        print(f"  Modalidade {cod_modalidade}: Fim dos resultados na página {pagina_atual}.")
                        break

                    for lic in licitacoes_pagina:
                        licitacoes_modalidade.append({
                            'id_pncp': lic.get('numeroControlePNCP'),
                            'ano': lic.get('anoCompra'),
                            'sequencial': lic.get('sequencialCompra'),
                            'modalidade_cod': cod_modalidade,
                            'modalidade_nome': lic.get('modalidadeNome'),
                            'objeto': lic.get('objetoCompra', ''),
                            'valor_total_estimado_licitacao': lic.get('valorTotalEstimado')
                        })

                    total_paginas_api = dados.get('totalPaginas', 1)
                    print(f"  Página {pagina_atual}/{total_paginas_api} processada.")
                    if pagina_atual >= total_paginas_api:
                         print(f"  Modalidade {cod_modalidade}: Última página alcançada.")
                         break

                elif response.status_code == 204:
                    print(f"  Nenhuma licitação encontrada para Modalidade {cod_modalidade} neste período.")
                    break
                else:
                    print(f"  Erro ao buscar Modalidade {cod_modalidade} (Página {pagina_atual}): Status {response.status_code}")
                    break

            except requests.Timeout:
                print(f"  TIMEOUT (Modalidade {cod_modalidade}, Página {pagina_atual}): A API demorou mais de 60 segundos para responder.")
                break
            except requests.RequestException as e:
                print(f"  Erro de conexão (Modalidade {cod_modalidade}, Página {pagina_atual}): {e}")
                break
            except json.JSONDecodeError:
                print(f"  Erro JSON (Modalidade {cod_modalidade}, Página {pagina_atual}).")
                break

        if licitacoes_modalidade:
            print(f"Modalidade {cod_modalidade}: Encontradas {len(licitacoes_modalidade)} licitações.")
            licitacoes_encontradas_total.extend(licitacoes_modalidade)

    print(f"\n--- Total de {len(licitacoes_encontradas_total)} licitações encontradas em todas as modalidades. ---")
    return licitacoes_encontradas_total

def buscar_itens_licitacao(cnpj, ano, sequencial):
    """Busca os itens de uma licitação específica."""
    print(f"  Buscando itens para licitação {ano}/{sequencial}...")
    itens_encontrados = []
    url_itens = f"{URL_API_PNCP_INTEGRACAO_BASE}/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    headers = {'Accept': 'application/json'}

    try:
        time.sleep(PAUSA_ENTRE_REQUISICOES_SEG)
        response = requests.get(url_itens, headers=headers, timeout=20)

        if response.status_code == 200:
            itens_bruto = response.json()
            for item in itens_bruto:
                itens_encontrados.append({
                    'numero_item': item.get('numeroItem'),
                    'tipo': item.get('materialOuServicoNome'), # Chave corrigida
                    'descricao': item.get('descricao', '').strip(),
                    'quantidade': item.get('quantidade'),
                    'valor_unit_estimado': item.get('valorUnitarioEstimado'),
                    'valor_total_estimado': item.get('valorTotalEstimado')
                })
            print(f"  Encontrados {len(itens_encontrados)} itens.")
        else:
            print(f"  Erro ao buscar itens para {ano}/{sequencial}: Status {response.status_code}")
    except requests.RequestException as e:
        print(f"  Erro de conexão ao buscar itens para {ano}/{sequencial}: {e}")
    except json.JSONDecodeError:
        print(f"  Erro ao decodificar JSON da API de itens para {ano}/{sequencial}.")

    return itens_encontrados


# --- FUNÇÃO DETECTOR DE INCONSISTÊNCIA ---
def detectar_inconsistencia_quantidade(descricao, quantidade):
    """
    Usa IA (spaCy) para verificar se a Quantidade=1 é inconsistente com a descrição.
    Retorna (str: motivo_aviso, bool: parar_comparacao)
    """
    # Se a quantidade for desconhecida (None) ou > 1, a heurística não se aplica.
    if quantidade is None or quantidade > 1:
        return None, False

    # A partir daqui, só analisamos itens com Quantidade == 1
    
    descricao_upper = descricao.upper()
    doc = NLP_MODEL(descricao_upper)
    
    # --- Heurística 1: Palavras-chave de Kit/Lote ---
    # (Verificamos a string original, antes da limpeza de palavras-chave)
    keywords_kit = {'KIT', 'CONJUNTO', 'CAIXA', 'PACOTE', 'LOTE', 'FARDO', 'JG', 'JOGO', 'CX', 'PCT'}
    for keyword in keywords_kit:
        # Usamos regex \b (word boundary) para evitar falsos positivos (ex: "caixão")
        if re.search(r'\b' + re.escape(keyword) + r'\b', descricao_upper):
            # Ex: "KIT DE FERRAMENTAS", Qtd: 1
            return f"Possível Kit/Caixa (palavra: '{keyword}')", True
            
    # --- Heurística 2: Substantivos no Plural ---
    substantivos_plurais = []
    
    for token in doc:
        # Verifica se o token é um Substantivo (NOUN)
        # E se sua análise morfológica indica Plural (Number=Plur)
        
        morph_number = token.morph.get("Number") # Retorna ['Plur'] ou ['Sing']
        
        if token.pos_ == 'NOUN' and morph_number and 'Plur' in morph_number:
            # Garante que não é um falso plural (ex: LÁPIS, ATLAS)
            # Se o lema (raiz) for diferente do texto, é um plural real.
            if token.lemma_.upper() != token.text.upper():
                substantivos_plurais.append(token.text)
                
    if substantivos_plurais:
        # Ex: "MESAS DE PLASTICO", Qtd: 1
        # Itens encontrados: ['MESAS']
        nomes_itens = list(dict.fromkeys(substantivos_plurais)) # Remove duplicatas
        return f"Possível Qtd incorreta (Item no plural: {', '.join(nomes_itens)})", True
            
    # Nenhuma inconsistência encontrada
    return None, False


# --- FUNÇÃO buscar_preco_varejo REFINADA COM IA (spaCy) ---
def buscar_preco_varejo(descricao_completa):
    """
    Busca o preço mediano de um item no Buscapé, usando IA (spaCy) para
    normalizar o nome do item e extrair palavras-chave relevantes.
    """
    if not descricao_completa:
        return None

    # --- INÍCIO DA NORMALIZAÇÃO COM IA ---
    # Coloca em maiúsculas (padrão de licitação) e processa com o modelo de NLP
    doc = NLP_MODEL(descricao_completa.upper())
    
    palavras_chave = []
    for token in doc:
        # Lematização: transforma a palavra na sua raiz (ex: "MESAS" -> "MESA")
        lemma = token.lemma_.upper() 
        
        # FILTROS INTELIGENTES:
        # 1. Não é uma "stopword" (ex: 'de', 'para', 'com')
        # 2. Não é uma palavra-chave de licitação (ex: 'AQUISIÇÃO', 'CONFORME')
        # 3. Não é pontuação (ex: ',', '.')
        # 4. Não é um número
        # 5. Tem mais de 2 letras
        # 6. É um Substantivo (NOUN), Nome Próprio (PROPN) ou Adjetivo (ADJ)
        #    (Isso foca a busca no *item* e suas *qualidades*)
        if (not token.is_stop and 
            lemma not in PALAVRAS_REMOVER_LICITACAO and 
            not token.is_punct and 
            not token.like_num and
            len(lemma) > 2 and
            token.pos_ in {'NOUN', 'PROPN', 'ADJ'}):
            
            palavras_chave.append(lemma)
    
    # Remove duplicatas mantendo a ordem
    palavras_unicas = list(dict.fromkeys(palavras_chave))
    
    # Pega as 5 primeiras palavras-chave mais relevantes
    termo_busca_lista = palavras_unicas[:5]
    
    if not termo_busca_lista:
        # Fallback: Se a IA não extrair nada (raro), usa o método antigo
        termo_busca = re.sub(r'[^\w\s]', '', descricao_completa)[:50].strip()
        print(f"      IA não extraiu termos. Usando fallback: '{termo_busca}'")
    else:
        termo_busca = " ".join(termo_busca_lista)
    # --- FIM DA NORMALIZAÇÃO COM IA ---

    termo_formatado = termo_busca.replace(' ', '+')
    url_buscape = f"https://www.buscape.com.br/search?q={termo_formatado}"

    print(f"      Buscando varejo para termo normalizado: '{termo_busca}' (Original: '{descricao_completa[:30]}...')...")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        time.sleep(PAUSA_ENTRE_REQUISICOES_SEG)
        response = requests.get(url_buscape, headers=headers, timeout=20)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Tenta múltiplos seletores comuns de preço
            seletores_preco = [
                'p[data-testid="product-card::price"]', 
                '[data-testid="product-price"]',
                '.Price_ValueContainer' # Seletor genérico
            ]
            elementos_de_preco = []
            for seletor in seletores_preco:
                elementos_de_preco.extend(soup.select(seletor))

            if not elementos_de_preco:
                print(f"      Nenhum preço encontrado no Buscapé para '{termo_busca}'.")
                return None

            precos_encontrados = []
            for el in elementos_de_preco:
                texto_preco = el.get_text()
                try:
                    # Regex mais robusto para extrair o número
                    match = re.search(r'[\d\.,]+', texto_preco.replace('.', ''))
                    if match:
                        preco_limpo = match.group(0).replace(',', '.')
                        preco_float = float(preco_limpo)
                        if preco_float > 0:
                            precos_encontrados.append(preco_float)
                except ValueError:
                    continue

            if precos_encontrados:
                df = pd.DataFrame(precos_encontrados, columns=['Preco_Varejo'])
                mediana = df['Preco_Varejo'].median()
                print(f"      Preço mediano no varejo: R$ {mediana:.2f} ({len(precos_encontrados)} amostras)")
                return mediana
            else:
                print(f"      Preços encontrados, mas não extraídos numericamente para '{termo_busca}'.")
                return None

        else:
            print(f"      Erro ao acessar Buscapé ({response.status_code}) para '{termo_busca}'. Possível bloqueio.")
            return None

    except requests.RequestException as e:
        print(f"      Erro de conexão com Buscapé para '{termo_busca}': {e}")
        return None
# --- FIM DA FUNÇÃO buscar_preco_varejo REFINADA ---


# --- FLUXO PRINCIPAL ATUALIZADO ---
if __name__ == "__main__":
    print("--- INICIANDO MONITOR DE LICITAÇÕES ---")

    licitacoes = buscar_licitacoes_recentes(CNPJ_AMARGOSA, DIAS_PARA_BUSCAR)

    if not licitacoes:
        print("\nNenhuma licitação encontrada ou erro na busca. Encerrando.")
        exit()

    print("\n--- Processando Itens e Comparando Preços ---")
    resultados_finais = []

    for lic in licitacoes:
        if not lic.get('ano') or not lic.get('sequencial'):
             print(f"\nLicitação {lic.get('id_pncp')} sem 'ano' ou 'sequencial'. Pulando busca de itens.")
             continue

        print(f"\nProcessando Licitação: {lic['ano']}/{lic['sequencial']} (Modalidade: {lic.get('modalidade_nome', lic['modalidade_cod'])}) - {lic['objeto'][:50]}...")

        itens = buscar_itens_licitacao(CNPJ_AMARGOSA, lic['ano'], lic['sequencial'])

        if not itens:
            print("  Nenhum item encontrado para esta licitação.")
            continue

        for item in itens:
            print(f"  Analisando Item {item['numero_item']}: '{item['descricao'][:60]}...' (Tipo: {item.get('tipo', 'Desconhecido')})")

            # --- OBTÉM OS DADOS ESSENCIAIS ---
            preco_estimado_lic = item.get('valor_unit_estimado')
            quantidade_lic = item.get('quantidade') # Pega a quantidade

            # --- Verificação de Inconsistência (Trava de Segurança) ---
            aviso_inconsistencia, parar_comparacao = detectar_inconsistencia_quantidade(item['descricao'], quantidade_lic)
            
            if aviso_inconsistencia:
                print(f"      ⚠️ AVISO: {aviso_inconsistencia}.")
                if preco_estimado_lic:
                     print(f"      O valor estimado (R$ {preco_estimado_lic:.2f}) pode ser referente ao Lote/Kit e não à unidade.")
            # --- FIM DA VERIFICAÇÃO ---

            preco_referencia = None
            fonte_referencia = None

            # --- LÓGICA DE COMPARAÇÃO ATUALIZADA ---
            
            # Se a heurística mandou parar, pulamos a busca de preço
            if parar_comparacao:
                print("      Comparação de preço de varejo PULADA devido à inconsistência de quantidade.")
                fonte_referencia = "N/A (Inconsistência Qtd/Descrição)"
            
            elif item.get('tipo') == 'Material':
                preco_referencia = buscar_preco_varejo(item['descricao']) 
                fonte_referencia = "Varejo (Buscapé/IA)"
            
            # <<< CORREÇÃO AQUI: Mudado de 'Servico' para 'Serviço' (com acento)
            elif item.get('tipo') == 'Serviço':
                print("      Item é um Serviço. Busca de preço de referência não aplicável (varejo).") 
                fonte_referencia = "N/A (Serviço)"
            
            else:
                print(f"      Tipo de item não identificado ou não é Material/Serviço: '{item.get('tipo')}'")
                fonte_referencia = "N/A (Tipo Desconhecido)"
            
            # --- FIM DA LÓGICA DE COMPARAÇÃO ---

            alerta = False
            diferenca_percentual = None

            if preco_estimado_lic is not None and preco_referencia is not None and preco_estimado_lic > 0 and preco_referencia > 0:
                diferenca_percentual = (preco_estimado_lic - preco_referencia) / preco_referencia

                if diferenca_percentual > LIMITE_ALERTA_PERCENTUAL:
                    alerta = True
                    print(f"      🚨 ALERTA! Preço estimado (R$ {preco_estimado_lic:.2f}) é {diferenca_percentual:.1%} acima da referência de {fonte_referencia} (R$ {preco_referencia:.2f})")
                else:
                    print(f"      Preço estimado (R$ {preco_estimado_lic:.2f}) está {diferenca_percentual:.1%} em relação à referência de {fonte_referencia} (R$ {preco_referencia:.2f})")
            
            elif preco_estimado_lic is not None and preco_estimado_lic > 0 and not preco_referencia:
                print(f"      Preço estimado: R$ {preco_estimado_lic:.2f}. Não foi possível obter preço de referência (Fonte: {fonte_referencia}).")
            
            elif preco_referencia is not None:
                print(f"      Preço de referência ({fonte_referencia}): R$ {preco_referencia:.2f}. Licitação não informou preço estimado unitário (>0).")
            
            elif not aviso_inconsistencia: 
                 print(f"      Não foi possível obter preço estimado da licitação (>0) nem preço de referência.")

            resultados_finais.append({
                'licitacao_id': f"{lic['ano']}/{lic['sequencial']}",
                'modalidade': lic.get('modalidade_nome', lic['modalidade_cod']),
                'item_num': item['numero_item'],
                'item_desc': item['descricao'],
                'item_tipo': item.get('tipo'),
                'item_quantidade_lic': quantidade_lic,
                'preco_estimado_lic': preco_estimado_lic,
                'preco_ref': preco_referencia,
                'fonte_ref': fonte_referencia,
                'diferenca_perc': diferenca_percentual,
                'alerta': alerta,
                'aviso_inSISTENCIA': aviso_inconsistencia
            })

    print("\n--- Monitoramento Concluído ---")

    if resultados_finais:
        df_resultados = pd.DataFrame(resultados_finais)
        
        colunas_ordem = [
            'licitacao_id', 'modalidade', 'item_num', 'item_desc', 'item_tipo', 
            'item_quantidade_lic', 'preco_estimado_lic', 'preco_ref', 'fonte_ref', 
            'diferenca_perc', 'alerta', 'aviso_inconsistencia'
        ]
        colunas_finais = [col for col in colunas_ordem if col in df_resultados.columns]
        df_resultados = df_resultados[colunas_finais]

        nome_arquivo = f"relatorio_monitoramento_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            df_resultados.to_csv(nome_arquivo, index=False, sep=';', decimal=',', encoding='utf-8-sig')
            print(f"\nRelatório salvo em: {nome_arquivo}")
        except Exception as e:
            print(f"\nErro ao salvar relatório CSV: {e}")