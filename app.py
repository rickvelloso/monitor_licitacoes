import os
from datetime import datetime

from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_caching import Cache
from flask_login import (LoginManager, UserMixin, current_user, login_required,
                         login_user, logout_user)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import check_password_hash, generate_password_hash

import buscador_pncp

# --- Configuração Base ---
app = Flask(__name__)
# Chave secreta necessária para formulários e sessões
app.config['SECRET_KEY'] = 'uma-chave-secreta-muito-dificil-de-adivinhar' 
basedir = os.path.abspath(os.path.dirname(__file__))

# --- Configuração do Banco de Dados SQLite ---
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'site_colaborativo.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Configuração do Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message = 'Por favor, faça login para acessar esta página.'

# --- Configuração do Cache ---
config_cache = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "cache"
}
app.config.from_mapping(config_cache)
cache = Cache(app)

# --- MODELOS DO BANCO DE DADOS ---
@login_manager.user_loader
def load_user(user_id):
    """Callback usado pelo Flask-Login para carregar um usuário da sessão."""
    return User.query.get(int(user_id))

class User(db.Model, UserMixin):
    """Tabela de Usuários"""
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128)) 

    contributions = db.relationship('Contribution', backref='author', lazy=True)
    sub_items = db.relationship('SubItem', backref='author', lazy=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def __repr__(self):
        return f'<User {self.username}>'


class Contribution(db.Model):
    """Tabela de Contribuições (Votos: Preço OK, Sobrepreço)"""
    id = db.Column(db.Integer, primary_key=True)
    
    # Cria uma chave única para cada item de licitação
    # Ex: "13825484000150-2025-240-1" (CNPJ-ANO-SEQ-ITEM_NUM)
    item_key = db.Column(db.String(300), nullable=False, index=True) 
    
    status = db.Column(db.String(50)) # Ex: "PRECO_OK", "SOBREPRECO"
    comment = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

    def __repr__(self):
        return f'<Contribution {self.item_key} - {self.status}>'


class SubItem(db.Model):
    """Tabela para itens cadastrados manualmente (para Lotes)"""
    id = db.Column(db.Integer, primary_key=True)
    # Chave do item pai ao qual este sub-item pertence
    parent_item_key = db.Column(db.String(300), nullable=False, index=True)
    
    descricao = db.Column(db.String(500), nullable=False)
    quantidade = db.Column(db.Integer)
    valor_unitario = db.Column(db.Float)
    
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<SubItem {self.descricao}>'

# --- FIM DOS MODELOS ---


# --- ROTAS DA APLICAÇÃO ---

@app.route("/")
def index():
    """Serve a página principal do aplicativo."""
    return render_template("index.html")

# (Aqui vamos adicionar as rotas /login, /register, /logout)
# ...


@app.route("/api/gerar-relatorio", methods=['GET'])
@cache.cached(timeout=43200, query_string=True) 
def api_relatorio():
    """
    API que busca os dados no PNCP, CRUZA com o banco de dados local
    e retorna o JSON enriquecido.
    """
    
    cnpj = request.args.get('cnpj')
    data_inicio = request.args.get('inicio')
    data_fim = request.args.get('fim')

    if not all([cnpj, data_inicio, data_fim]):
        return jsonify({"erro": "Parâmetros 'cnpj', 'inicio' e 'fim' são obrigatórios"}), 400

    try:
        print(f"Iniciando busca no PNCP para CNPJ: {cnpj}...")
        # 1. Busca os dados brutos do PNCP
        itens_pncp = buscador_pncp.gerar_relatorio_bruto(cnpj, data_inicio, data_fim)
        
        # 2. ENRIQUECER OS DADOS
        # Pega todas as chaves de itens que vieram do PNCP
        item_keys = [
            f"{item['cnpj']}-{item['ano']}-{item['sequencial']}-{item['numero_item']}" 
            for item in itens_pncp
        ]
        
        # Busca em nosso banco de dados todas as contribuições e sub-itens
        # para *qualquer* um desses itens
        contribuicoes = db.session.scalars(
            db.select(Contribution).where(Contribution.item_key.in_(item_keys))
        ).all()
        
        sub_itens = db.session.scalars(
            db.select(SubItem).where(SubItem.parent_item_key.in_(item_keys))
        ).all()
        
        # Organiza em dicionários para consulta rápida
        contrib_map = {}
        for c in contribuicoes:
            if c.item_key not in contrib_map:
                contrib_map[c.item_key] = []
            contrib_map[c.item_key].append({'status': c.status, 'comment': c.comment})
            
        sub_item_map = {}
        for s in sub_itens:
            if s.parent_item_key not in sub_item_map:
                sub_item_map[s.parent_item_key] = []
            sub_item_map[s.parent_item_key].append({
                'descricao': s.descricao, 
                'quantidade': s.quantidade,
                'valor_unitario': s.valor_unitario
            })
        
        # 3. Adiciona os dados colaborativos em cada item do PNCP
        itens_enriquecidos = []
        for item in itens_pncp:
            key = f"{item['cnpj']}-{item['ano']}-{item['sequencial']}-{item['numero_item']}"
            item['item_key'] = key 
            item['contribuicoes'] = contrib_map.get(key, []) # Adiciona votos
            item['sub_itens'] = sub_item_map.get(key, []) # Adiciona itens do lote
            itens_enriquecidos.append(item)
            
        print("Busca concluída, retornando JSON enriquecido.")
        return jsonify(itens_enriquecidos)
    
    except Exception as e:
        print(f"Erro ao processar API: {e}")
        # Desabilita o cache em caso de erro para não salvar um resultado ruim
        cache.delete_memoized(api_relatorio) 
        return jsonify({"erro": f"Erro interno no servidor: {str(e)}"}), 500

# (Aqui vamos adicionar a rota /api/contribuir)
# ...


if __name__ == '__main__':
    app.run(debug=True)