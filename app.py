import os
from datetime import datetime

from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   url_for)
from flask_caching import Cache
from flask_login import (LoginManager, UserMixin, current_user, login_required,
                         login_user, logout_user)
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from werkzeug.security import check_password_hash, generate_password_hash
from wtforms import PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, ValidationError

import buscador_pncp

# --- Configuração Base ---
app = Flask(__name__)
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
# Mensagem em português
login_manager.login_message = 'Por favor, faça login para acessar esta página.'

# --- Configuração do Cache (sem mudança) ---
config_cache = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "cache"
}
app.config.from_mapping(config_cache)
cache = Cache(app)

# --- FIM DAS CONFIGURAÇÕES ---


# --- MODELOS DO BANCO DE DADOS (Sem mudança) ---

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    telefone = db.Column(db.String(20), unique=True, nullable=False, index=True)
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
    id = db.Column(db.Integer, primary_key=True)
    item_key = db.Column(db.String(300), nullable=False, index=True) 
    status = db.Column(db.String(50)) 
    comment = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    def __repr__(self):
        return f'<Contribution {self.item_key} - {self.status}>'

class SubItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    parent_item_key = db.Column(db.String(300), nullable=False, index=True)
    descricao = db.Column(db.String(500), nullable=False)
    quantidade = db.Column(db.Integer)
    valor_unitario = db.Column(db.Float)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    def __repr__(self):
        return f'<SubItem {self.descricao}>'

# --- FIM DOS MODELOS ---


# --- NOVAS CLASSES DE FORMULÁRIO ---

class RegistrationForm(FlaskForm):
    """Formulário de Registro com Username, Email e Telefone"""
    username = StringField('Nome de usuário', validators=[DataRequired()])
    email = StringField('E-mail', validators=[DataRequired(), Email(message="E-mail inválido.")])
    telefone = StringField('Telefone (com DDD)', validators=[DataRequired()])
    password = PasswordField('Senha', validators=[DataRequired()])
    password2 = PasswordField(
        'Confirmar Senha', validators=[DataRequired(), EqualTo('password', message='As senhas devem ser iguais.')])
    submit = SubmitField('Registrar')

    def validate_username(self, username):
        user = db.session.scalar(db.select(User).where(User.username == username.data))
        if user is not None:
            raise ValidationError('Este nome de usuário já está em uso.')

    def validate_email(self, email):
        user = db.session.scalar(db.select(User).where(User.email == email.data))
        if user is not None:
            raise ValidationError('Este e-mail já está em uso.')

    def validate_telefone(self, telefone):
        user = db.session.scalar(db.select(User).where(User.telefone == telefone.data))
        if user is not None:
            raise ValidationError('Este telefone já está em uso.')

class LoginForm(FlaskForm):
    """Formulário de Login com Username (Revertido)"""
    username = StringField('Nome de usuário', validators=[DataRequired()])
    password = PasswordField('Senha', validators=[DataRequired()])
    submit = SubmitField('Login')

# --- FIM DOS FORMULÁRIOS ---


# --- ROTAS DA APLICAÇÃO ---

@app.route("/")
def index():
    """Serve a página principal do aplicativo."""
    return render_template("index.html")

# --- NOVAS ROTAS DE USUÁRIO ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    
    form = LoginForm()
    if form.validate_on_submit():
        user = db.session.scalar(db.select(User).where(User.username == form.username.data))
        
        if user is None or not user.check_password(form.password.data):
            flash('Nome de usuário ou senha inválidos', 'error')
            return redirect(url_for('login'))
        
        login_user(user) 
        flash('Login realizado com sucesso!', 'success')
        return redirect(url_for('index'))
    
    return render_template('login.html', title='Login', form=form)

@app.route('/logout')
def logout():
    """Lida com o logout do usuário"""
    logout_user()
    flash('Você foi desconectado.', 'success')
    return redirect(url_for('index'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(
            username=form.username.data, 
            email=form.email.data, 
            telefone=form.telefone.data
        )
        user.set_password(form.password.data)
        db.session.add(user)
        db.session.commit()
        flash('Parabéns, você foi registrado com sucesso! Faça o login.', 'success')
        return redirect(url_for('login'))
    
    return render_template('register.html', title='Registrar', form=form)

# --- FIM DAS ROTAS DE USUÁRIO ---


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
        itens_pncp = buscador_pncp.gerar_relatorio_bruto(cnpj, data_inicio, data_fim)
        
        if not itens_pncp:
             return jsonify([]) # Retorna lista vazia se nada for encontrado

        print("Buscando dados colaborativos...")
        item_keys = [
            f"{item['cnpj']}-{item['ano']}-{item['sequencial']}-{item['numero_item']}" 
            for item in itens_pncp if item.get('cnpj') # Garante que os dados estão lá
        ]
        
        contribuicoes = db.session.scalars(
            db.select(Contribution).where(Contribution.item_key.in_(item_keys))
        ).all()
        
        sub_itens = db.session.scalars(
            db.select(SubItem).where(SubItem.parent_item_key.in_(item_keys))
        ).all()
        
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
        
        itens_enriquecidos = []
        for item in itens_pncp:
            key = f"{item.get('cnpj')}-{item.get('ano')}-{item.get('sequencial')}-{item.get('numero_item')}"
            item['item_key'] = key
            item['contribuicoes'] = contrib_map.get(key, []) 
            item['sub_itens'] = sub_item_map.get(key, []) 
            itens_enriquecidos.append(item)
            
        print("Busca concluída, retornando JSON enriquecido.")
        return jsonify(itens_enriquecidos)
    
    except Exception as e:
        print(f"Erro ao processar API: {e}")
        cache.delete_memoized(api_relatorio) 
        return jsonify({"erro": f"Erro interno no servidor: {str(e)}"}), 500
    
@app.route('/api/contribuir', methods=['POST'])
@login_required # Garante que só usuários logados podem chamar esta API
def api_contribuir():
    """Recebe a contribuição (voto) de um usuário."""
    data = request.json
    
    # Validação simples dos dados recebidos do frontend
    item_key = data.get('item_key')
    status = data.get('status')
    link = data.get('link')
    comment = data.get('comment')
    
    if not all([item_key, status, link]):
        return jsonify({"erro": "Dados incompletos. Chave, status e link são obrigatórios."}), 400
        
    if status not in ["SOBREPRECO", "PRECO_OK", "ABAIXO_PRECO"]:
        return jsonify({"erro": "Status de voto inválido."}), 400

    try:
        # Cria o novo registro de Contribuição no banco
        nova_contribuicao = Contribution(
            item_key=item_key,
            status=status,
            link=link,
            comment=comment,
            user_id=current_user.id  # Associa ao usuário logado
        )
        db.session.add(nova_contribuicao)
        db.session.commit()
        
        # --- MUITO IMPORTANTE ---
        # Limpa o cache para que a próxima busca já mostre o novo voto.
        cache.clear()
        
        print(f"Nova contribuição registrada por {current_user.username} para o item {item_key}")
        return jsonify({"sucesso": "Contribuição registrada com sucesso!"}), 201

    except Exception as e:
        db.session.rollback()
        print(f"Erro ao registrar contribuição: {e}")
        return jsonify({"erro": "Erro interno ao salvar a contribuição."}), 500
    
    


if __name__ == '__main__':
    # Cria o banco de dados se ele não existir
    with app.app_context():
        db.create_all()
    app.run(debug=True)