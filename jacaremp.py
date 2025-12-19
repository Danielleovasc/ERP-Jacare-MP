import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import OperationalError
import psycopg2.extras
import hashlib
import base64 
from dotenv import load_dotenv
import os


load_dotenv()

# --- 1. CONFIGURAÇÃO DO BANCO DE DADOS ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST'), 
    'port': os.getenv('DB_PORT'),  
    'user': os.getenv('DB_USER'),  
    'password': os.getenv('DB_PASSWORD'), 
    'dbname': os.getenv('DB_NAME'),  
}

#----------------------------------------------------------------------------------------------------------------

# --- 2. FUNÇÕES DE CONEXÃO E UTILITÁRIOS DB ---
@st.cache_resource
def get_db_connection_cached():
    """Retorna uma conexão CACHEADA para LEITURAS (fetch)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except OperationalError as e:
        st.error(f"Erro ao conectar ao Supabase (CACHE): {e}")
        st.stop()
        return None

def get_db_connection_new():
    """Retorna uma conexão NOVA para TRANSAÇÕES (compras, pedidos)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except OperationalError as e:
        st.error(f"Erro ao conectar ao Supabase (NOVA CONEXÃO): {e}")
        return None

def execute_query(sql, params=None, fetch=False):
    """Executa comandos SQL e gerencia commit/rollback para operações simples."""
    conn = get_db_connection_cached()
    if conn is None:
        return None

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    result = None
    
    try:
        cursor.execute(sql, params)
        
        if sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            conn.commit()
            # Limpa o cache se algo foi alterado
            if sql.strip().upper().startswith("INSERT INTO Produtos"):
                get_db_connection_cached.clear()
            return True
        
        if fetch:
            result = cursor.fetchall()
            return result
        
        return True
    
    except OperationalError as e:
        st.error(f"Erro ao executar a query: {e}. SQL: {sql}")
        if conn:
            conn.rollback() 
        return None
    finally:
        cursor.close()

def fetch_all(table_name):
    """Busca todos os registros de uma tabela."""
    sql = f"SELECT * FROM {table_name}"
    data = execute_query(sql, fetch=True)
    return pd.DataFrame(data) if data else pd.DataFrame()

def fetch_data_for_display(table_name, columns, join_info=None, condition=None, params=None):
    """Função genérica para buscar dados com joins e condição WHERE para exibição."""
    select_cols = ", ".join(columns)
    sql = f"SELECT {select_cols} FROM {table_name}"
    
    if join_info:
        for join in join_info:
            sql += f" LEFT JOIN {join['table']} ON {join['on']}"
            
    if condition:
        sql += f" WHERE {condition}"
            
    data = execute_query(sql, params=params, fetch=True)
    return pd.DataFrame(data) if data else pd.DataFrame()
#-------------------------------------------------------------------------------------------------------------------------------------------

# --- FUNÇÕES DE CUPOM NÃO FISCAL (NOVAS) ---

def get_order_details_for_coupon(pedido_id):
    """Busca os detalhes completos de um pedido (cabeçalho e itens) para impressão do cupom."""
    
    # SQL para buscar dados do cabeçalho do pedido
    sql_header = """
        SELECT P.pedido_id, C.nome AS cliente_nome, C.cpf_cnpj, 
               P.data_pedido, P.valor_total, P.forma_pagamento
        FROM Pedidos P
        LEFT JOIN Clientes C ON P.cliente_id = C.cliente_id
        WHERE P.pedido_id = %s
    """
    header_data = execute_query(sql_header, params=(pedido_id,), fetch=True)
    
    if not header_data:
        return None

    # SQL para buscar os itens do pedido
    sql_items = """
        SELECT I.quantidade, I.preco_unitario, I.subtotal, 
               Pr.descricao
        FROM Vendas I
        LEFT JOIN Produtos Pr ON I.produto_id = Pr.produto_id
        WHERE I.pedido_id = %s
    """
    items_data = execute_query(sql_items, params=(pedido_id,), fetch=True)
    
    return {
        'header': header_data[0],
        'items': items_data
    }


def generate_non_fiscal_coupon(pedido_id, order_details):
    """
    Gera o conteúdo HTML/Markdown do cupom não fiscal.
    Usa um bloco de código pré-formatado e HTML para simular uma impressão.
    """
    
    header = order_details['header']
    items = order_details['items']
    
    # Formata a data e hora
    data_formatada = header['data_pedido'].strftime('%d/%m/%Y %H:%M') if isinstance(header['data_pedido'], datetime) else str(header['data_pedido'])
    
    # Conteúdo HTML/CSS para simular o cupom de forma estreita
    coupon_content = f"""
<div style="font-family: monospace; font-size: 10px; line-height: 1.2; width: 300px; margin: 0 auto; padding: 10px; border: 1px dashed black; background-color: #fff;">
    <h3 style="text-align: center; margin-bottom: 5px;">AUTOPEÇAS JACARÉ 🐊</h3>
    <p style="text-align: center; margin: 0;">CNPJ: 00.000.000/0001-00</p>
    <p style="text-align: center; margin: 0;">Rua Exemplo, 123 - Centro</p>
    <p style="text-align: center; margin-bottom: 10px;">(92) 99999-9999</p>
    
    <p style="border-top: 1px dashed black; padding-top: 5px; margin: 5px 0 5px 0;">
        **CUPOM NÃO FISCAL**<br>
        PEDIDO: **#{header['pedido_id']}**<br>
        DATA: {data_formatada}<br>
        CLIENTE: {header['cliente_nome']}<br>
        CPF/CNPJ: {header['cpf_cnpj'] if header['cpf_cnpj'] else 'Não Informado'}<br>
    </p>
    <p style="border-top: 1px dashed black; padding-top: 5px; margin: 5px 0;">
        **ITENS DA VENDA:**<br>
        DESCRIÇÃO | QTD | UNIT (R$) | TOTAL (R$)<br>
        -------------------------------------------
    </p>
    """
    
    # 2. Itens
    for item in items:
        # Formatação para simular coluna fixa
        nome = item['descricao'][:15].ljust(15) 
        qtd = f"{item['quantidade']:.0f}".rjust(3)
        unit = f"{item['preco_unitario']:.2f}".rjust(9)
        total = f"{item['subtotal']:.2f}".rjust(9)
        
        coupon_content += f"""
        <p style="margin: 0;">{nome} | {qtd} | {unit} | {total}</p>
        """
        
    # 3. Totais e Fechamento
    coupon_content += f"""
    <p style="border-top: 1px dashed black; padding-top: 5px; margin: 5px 0;">
        VALOR TOTAL: {f"R$ {header['valor_total']:.2f}".rjust(26)}<br>
        FORMA PGTO: {header['forma_pagamento']}<br>
    </p>
    <p style="border-top: 1px dashed black; padding-top: 5px; text-align: center;">
        *** OBRIGADO PELA PREFERÊNCIA! ***<br>
        Este cupom não possui validade fiscal.<br>
        Verifique a garantia de seus produtos.
    </p>
</div>
    """
    return coupon_content


#-------------------------------------------------------------------------------------------------------------------------------------------

# --- 3. INTERFACE STREAMLIT (MÓDULOS) ---

st.set_page_config(layout="wide", page_title="ERP de Peças de Motos (Supabase)")

st.markdown(
    """
    <h1 style="text-align: center;">🏍️ Moto Peças Jacaré 🐊</h1>
    """,
    unsafe_allow_html=True
)

# NOVO MENU: Incluindo o módulo de Despesas
menu = [ 
    "Clientes", 
    "Fornecedores", 
    "Categorias", 
    "Produtos (Estoque)", 
    "Compras e Recebimento de Estoque", 
    "Pedidos de Venda", 
    "Devoluções",
    "Despesas e Fluxo de Caixa"
]

choice = st.sidebar.selectbox(
    "Módulos do Sistema",
    menu,
    index=None,
    placeholder="Selecione um módulo",
)


# --- ADIÇÃO DA LOGOMARCA NO CENTRO INFERIOR DA BARRA LATERAL (CORRIGIDO) ---
st.sidebar.markdown("---") 
try:
    # 1. HTML/CSS para fixar e centralizar a imagem no final da barra lateral
    st.markdown(
        f"""
        <style>
            [data-testid="stSidebar"] {{
                /* Define a barra lateral como um contêiner flexível */
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            }}
            .sidebar-footer {{
                /* Estilo para a div que contém a imagem */
                padding: 10px;
                text-align: center;
                margin-top: auto; 
            }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.sidebar.image("logomarca.png", use_column_width=True) 

except FileNotFoundError:
    st.sidebar.warning("Arquivo 'logomarca.png' não encontrado. Verifique o caminho.")

#-------------------------------------------------------------------------------------------------------------------------------------------

# ------------ CADASTRO DE CLIENTES -------------------
if choice == "Clientes":
    # ... (código Clientes) ...
    st.header("Cadastro de Clientes")
    with st.expander("➕ Novo Cadastro"):
        with st.form("form_cliente"):
            nome = st.text_input("Nome Completo", key='nome_c')
            col_c1, col_c2 = st.columns(2)
            cpf_cnpj = col_c1.text_input("CPF/CNPJ", key='doc_c')
            telefone = col_c2.text_input("Telefone", key='tel_c')
            email = st.text_input("E-mail", key='mail_c')
            endereco = st.text_area("Endereço Completo", key='end_c')
            data_cadastro = datetime.now().strftime("%Y-%m-%d")
            
            submitted = st.form_submit_button("Salvar Cliente")
            
            if submitted:
                if nome and cpf_cnpj:
                    sql = """
                        INSERT INTO Clientes (nome, cpf_cnpj, telefone, email, endereco, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    params = (nome, cpf_cnpj, telefone, email, endereco, data_cadastro)
                    if execute_query(sql, params):
                        st.success(f"Cliente '{nome}' cadastrado com sucesso no DB!")
                        st.rerun()
                    else:
                        st.error("Falha ao cadastrar cliente. Verifique se o CPF/CNPJ ou E-mail já existe.")
                else:
                    st.error("Nome e CPF/CNPJ são obrigatórios.")

    st.subheader("Lista de Clientes")
    df_clientes = fetch_all("Clientes")
    if not df_clientes.empty:
        st.dataframe(df_clientes)
    else:
        st.info("Nenhum cliente cadastrado no banco de dados.")

#-------------------------------------------------------------------------------------------------------------------------------------------

# ------------ CADASTRO DE FORNECEDORES -------------------
elif choice == "Fornecedores":
    # ... (código Fornecedores) ...
    st.header("Cadastro de Fornecedores")
    
    with st.expander("➕ Novo Fornecedor"):
        with st.form("form_fornecedor"):
            nome_fantasia = st.text_input("Nome Fantasia", key='nome_f')
            cnpj = st.text_input("CNPJ", key='cnpj_f')
            col_f1, col_f2 = st.columns(2)
            telefone = col_f1.text_input("Telefone", key='tel_f')
            email = col_f2.text_input("E-mail", key='mail_f')
            contato = st.text_input("Pessoa de Contato", key='contato_f')
            
            submitted = st.form_submit_button("Salvar Fornecedor")
            
            if submitted:
                if nome_fantasia and cnpj:
                    sql = """
                        INSERT INTO Fornecedores (nome_fantasia, cnpj, telefone, email, contato)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    params = (nome_fantasia, cnpj, telefone, email, contato)
                    if execute_query(sql, params):
                        st.success(f"Fornecedor '{nome_fantasia}' cadastrado com sucesso no DB!")
                        st.rerun()
                    else:
                        st.error("Falha ao cadastrar fornecedor. Verifique se o CNPJ ou E-mail já existe.")
                else:
                    st.error("Nome Fantasia e CNPJ são obrigatórios.")

    st.subheader("Lista de Fornecedores")
    df_fornecedores = fetch_all("Fornecedores")
    if not df_fornecedores.empty:
        st.dataframe(df_fornecedores)
    else:
        st.info("Nenhum fornecedor cadastrado no banco de dados.")

#-------------------------------------------------------------------------------------------------------------------------------------------

# ------------ CADASTRO DE CATEGORIAS -------------------
elif choice == "Categorias":
    # ... (código Categorias) ...
    st.header("Cadastro de Categorias de Peças")
    
    with st.expander("➕ Nova Categoria"):
        with st.form("form_categoria"):
            nome_categoria = st.text_input("Nome da Categoria (Ex: Motor, Suspensão, Elétrica)", key='nome_cat')
            
            submitted = st.form_submit_button("Salvar Categoria")
            
            if submitted:
                if nome_categoria:
                    sql = "INSERT INTO Categorias (nome_categoria) VALUES (%s)"
                    params = (nome_categoria,)
                    if execute_query(sql, params):
                        st.success(f"Categoria '{nome_categoria}' cadastrada com sucesso!")
                        st.rerun() 
                    else:
                        st.error("Falha ao cadastrar categoria. Verifique se a categoria já existe.")
                else:
                    st.error("O nome da categoria é obrigatório.")

    st.subheader("Lista de Categorias")
    df_categorias = fetch_all("Categorias")
    if not df_categorias.empty:
        st.dataframe(df_categorias)
    else:
        st.info("Nenhuma categoria cadastrada no banco de dados.")

#-------------------------------------------------------------------------------------------------------------------------------------------

# ------------ CADASTRO DE PRODUTOS -------------------
elif choice == "Produtos (Estoque)":
    st.header("Cadastro de Peças e Controle de Estoque")
    
    df_categorias = fetch_all("Categorias")
    df_fornecedores = fetch_all("Fornecedores")
    df_produtos_full = fetch_all("Produtos") # Buscando produtos para a lista de preços
    
    if df_categorias.empty or df_fornecedores.empty:
        st.warning("⚠️ Atenção! É necessário cadastrar Categorias e Fornecedores antes de cadastrar Produtos.")
    else:
        opcoes_categorias = dict(zip(df_categorias['nome_categoria'], df_categorias['categoria_id']))
        opcoes_fornecedores = dict(zip(df_fornecedores['nome_fantasia'], df_fornecedores['fornecedor_id']))
        
        # --- BLOCO 1: NOVO PRODUTO ---
        with st.expander("➕ Novo Produto (Cadastro Inicial)"):
            with st.form("form_produto"):
                codigo_sku = st.text_input("Código SKU", key='sku_p')
                descricao = st.text_input("Descrição da Peça", key='desc_p')
                marca = st.text_input("Marca", key='marca_p')
                
                col_p1, col_p2, col_p3, col_p4 = st.columns(4)
                preco_custo = col_p1.number_input("Preço Custo (R$)", min_value=0.0, format="%.2f", key='custo_p')
                preco_venda = col_p2.number_input("Preço Venda (R$)", min_value=0.0, format="%.2f", key='venda_p')
                estoque_atual = col_p3.number_input("Estoque Atual", min_value=0, step=1, key='atual_p')
                estoque_minimo = col_p4.number_input("Estoque Mínimo", min_value=0, step=1, key='min_p')
                
                col_p5, col_p6 = st.columns(2)
                categoria_selecionada = col_p5.selectbox("Categoria", list(opcoes_categorias.keys()), key='cat_p')
                fornecedor_selecionado = col_p6.selectbox("Fornecedor", list(opcoes_fornecedores.keys()), key='forn_p')

                col_p7, col_p8 = st.columns(2)
                modelo_moto = col_p7.text_input("Modelo da Moto (Ex: Honda CB 300R)", key='mod_p')
                ano_moto = col_p8.text_input("Ano da Moto (Ex: 2012)", key='ano_p')
                
                submitted = st.form_submit_button("Salvar Produto (Cadastro Novo)")
                
                if submitted:
                    if codigo_sku and marca:
                        cat_id = opcoes_categorias[categoria_selecionada]
                        forn_id = opcoes_fornecedores[fornecedor_selecionado]
                        
                        sql = """
                            INSERT INTO Produtos (codigo_sku, descricao, marca, preco_custo, preco_venda, estoque_atual, estoque_minimo, categoria_id, fornecedor_id, modelo_moto, ano_moto)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        params = (codigo_sku, descricao, marca, preco_custo, preco_venda, int(estoque_atual), int(estoque_minimo), cat_id, forn_id, modelo_moto, ano_moto)
                        
                        if execute_query(sql, params):
                            st.success(f"Produto SKU '{codigo_sku}' cadastrado com sucesso!")
                            st.rerun()
                        else:
                            st.error("Erro ao cadastrar. Verifique se o SKU já existe.")

        # --- BLOCO 2: ALTERAÇÃO DE PREÇOS ---
        with st.expander("💰 Atualizar Preços de Produtos Existentes"):
            if df_produtos_full.empty:
                st.info("Nenhum produto cadastrado para alterar preços.")
            else:
                # Criar uma lista de seleção formatada: "SKU - Descrição (Marca)"
                df_produtos_full['display_name'] = df_produtos_full['codigo_sku'] + " - " + df_produtos_full['descricao'] + " (" + df_produtos_full['marca'] + ")"
                opcoes_alterar_preco = dict(zip(df_produtos_full['display_name'], df_produtos_full['produto_id']))
                
                with st.form("form_alterar_preco"):
                    produto_selecionado_label = st.selectbox("Selecione o Produto para Alterar Preço", list(opcoes_alterar_preco.keys()))
                    produto_id_update = opcoes_alterar_preco[produto_selecionado_label]
                    
                    # Pegar dados atuais do produto selecionado para preencher os campos
                    dados_atuais = df_produtos_full[df_produtos_full['produto_id'] == produto_id_update].iloc[0]
                    
                    col_u1, col_u2 = st.columns(2)
                    novo_preco_custo = col_u1.number_input("Novo Preço Custo (R$)", min_value=0.0, format="%.2f", value=float(dados_atuais['preco_custo']))
                    novo_preco_venda = col_u2.number_input("Novo Preço Venda (R$)", min_value=0.0, format="%.2f", value=float(dados_atuais['preco_venda']))
                    
                    btn_update_preco = st.form_submit_button("Atualizar Preços")
                    
                    if btn_update_preco:
                        sql_update = "UPDATE Produtos SET preco_custo = %s, preco_venda = %s WHERE produto_id = %s"
                        if execute_query(sql_update, (novo_preco_custo, novo_preco_venda, produto_id_update)):
                            st.success(f"Preços de '{produto_selecionado_label}' atualizados!")
                            st.rerun()
                        else:
                            st.error("Erro ao atualizar preços.")

        # --- BLOCO 3: VISUALIZAÇÃO DA TABELA ---
        st.subheader("Estoque Atual de Produtos")
        
        join_columns = [
            "P.codigo_sku", "P.marca", "P.descricao", "P.ano_moto",
            "P.preco_custo", "P.preco_venda", "P.estoque_atual", "P.estoque_minimo", 
            "C.nome_categoria", "F.nome_fantasia AS fornecedor", "P.ativo as status",
        ]
        join_info = [
            {'table': 'Categorias C', 'on': 'P.categoria_id = C.categoria_id'},
            {'table': 'Fornecedores F', 'on': 'P.fornecedor_id = F.fornecedor_id'}
        ]
        
        df_produtos_display = fetch_data_for_display("Produtos P", join_columns, join_info)
        
        if not df_produtos_display.empty:
            st.dataframe(df_produtos_display)
        else:
            st.info("Nenhum produto cadastrado no banco de dados.")
#-------------------------------------------------------------------------------------------------------------------------------------------

# --- MÓDULO: COMPRAS E RECEBIMENTO DE ESTOQUE ---
elif choice == "Compras e Recebimento de Estoque":
    st.header("📦 Registro de Compras e Recebimento de Mercadorias")
    
    df_fornecedores = fetch_all("Fornecedores")
    df_produtos = fetch_all("Produtos")

    if df_fornecedores.empty or df_produtos.empty:
        st.warning("⚠️ É necessário ter Fornecedores e Produtos cadastrados para registrar entradas.")
    else:
        opcoes_fornecedores = dict(zip(df_fornecedores['nome_fantasia'], df_fornecedores['fornecedor_id']))
        opcoes_produtos = dict(zip(df_produtos['descricao'], df_produtos['produto_id']))
        
        with st.form("form_entrada_estoque"):
            st.subheader("Lançamento de Nota Fiscal/Compra")
            
            col_e0, col_e0_5 = st.columns(2)
            fornecedor_selecionado = col_e0.selectbox(
                "Fornecedor",
                options=list(opcoes_fornecedores.keys()),
                index=None,
                placeholder="Selecione o fornecedor",
                key="forn_entrada"
            )
            nota_fiscal = col_e0_5.text_input("Número da Nota Fiscal (NF)", key='nf_entrada')
            
            produto_selecionado = st.selectbox(
                "Produto Recebido",
                options=list(opcoes_produtos.keys()),
                index=None,
                placeholder="Selecione o produto",
                key="prod_entrada"
            )
            
            col_e1, col_e2, col_e3, col_e4 = st.columns(4)

            quantidade_entrada = col_e1.number_input("Qtd Comprada", min_value=1, step=1, key='qtd_entrada')

            valor_unitario_compra = col_e2.number_input(
                "Valor Unitário (R$)",
                min_value=0.0,
                format="%.2f",
                key="valor_unitario_compra"
            )

            # Campo de data na interface
            data_emissao_input = col_e3.date_input("Data de Emissão", datetime.now().date(), key='data_emissao_nf')
            data_recebimento_input = col_e4.date_input("Data Recebimento", datetime.now().date(), key='data_entrada')
            
            submitted = st.form_submit_button("Registrar Compra e Dar Entrada no Estoque", type="primary")
            
            if submitted:
                if fornecedor_selecionado and produto_selecionado and valor_unitario_compra is not None:
                    
                    produto_id = opcoes_produtos[produto_selecionado]
                    fornecedor_id = opcoes_fornecedores[fornecedor_selecionado]
                    
                    conn = get_db_connection_new()
                    if conn is None:
                        st.error("Conexão com o banco de dados indisponível.")
                        st.stop()
                        
                    cursor = conn.cursor()

                    try:
                        # 1. Busca dados atuais (Corrigido para usar índices numéricos)
                        sql_dados_atuais = "SELECT estoque_atual, preco_custo FROM Produtos WHERE produto_id = %s"
                        cursor.execute(sql_dados_atuais, (produto_id,))
                        resultado = cursor.fetchone()
                        
                        if not resultado:
                            st.error("Produto não encontrado no DB.")
                            st.stop()
                            
                        estoque_atual_db = int(resultado[0]) if resultado[0] is not None else 0
                        custo_atual_db = float(resultado[1]) if resultado[1] is not None else 0.0
                        
                        quantidade_entrada_int = int(quantidade_entrada)
                        
                        # --- TRANSAÇÃO 1: REGISTRO NO HISTÓRICO (Coluna: emissao) ---
                        sql_insert_compra = """
                            INSERT INTO Entradas (produto_id, fornecedor_id, data_recebimento, emissao, quantidade_comprada, valor_unitario_compra, numero_nota_fiscal)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        params_compra = (
                            produto_id, fornecedor_id, 
                            data_recebimento_input, data_emissao_input, 
                            quantidade_entrada_int, 
                            valor_unitario_compra, nota_fiscal
                        )
                        cursor.execute(sql_insert_compra, params_compra)
                        
                        # --- CÁLCULO DO CUSTO MÉDIO ---
                        valor_total_antigo = estoque_atual_db * custo_atual_db
                        valor_total_novo = quantidade_entrada_int * valor_unitario_compra
                        novo_estoque = estoque_atual_db + quantidade_entrada_int
                        
                        if novo_estoque > 0:
                            novo_custo_medio = (valor_total_antigo + valor_total_novo) / novo_estoque
                        else:
                            novo_custo_medio = valor_unitario_compra

                        # --- TRANSAÇÃO 2: ATUALIZAÇÃO DO PRODUTO ---
                        sql_update_estoque = """
                            UPDATE Produtos 
                            SET estoque_atual = %s, preco_custo = %s 
                            WHERE produto_id = %s
                        """
                        cursor.execute(sql_update_estoque, (novo_estoque, round(novo_custo_medio, 4), produto_id))
                        
                        conn.commit()
                        st.success(f"Estoque atualizado! Novo saldo: {novo_estoque}")
                        st.rerun()

                    except Exception as e:
                        if conn: conn.rollback()
                        st.error(f"Erro ao processar transação: {e}")
                    finally:
                        if cursor: cursor.close()
                        if conn: conn.close() 

        st.markdown("---")
        st.subheader("Histórico de Entradas de Estoque")

        # Colunas para o Histórico (ajustado para 'emissao')
        historico_cols = [
            "F.nome_fantasia AS Fornecedor", 
            "P.descricao AS Produto", 
            "E.emissao", "E.data_recebimento", 
            "E.quantidade_comprada", "E.valor_unitario_compra", 
            "E.numero_nota_fiscal"
        ]
        historico_join = [
            {'table': 'Produtos P', 'on': 'E.produto_id = P.produto_id'},
            {'table': 'Fornecedores F', 'on': 'E.fornecedor_id = F.fornecedor_id'}
        ]
        
        df_historico = fetch_data_for_display("Entradas E", historico_cols, historico_join)

        if not df_historico.empty:
            # Formatação das colunas de data
            for col_data in ['emissao', 'data_recebimento']:
                if col_data in df_historico.columns:
                    df_historico[col_data] = pd.to_datetime(df_historico[col_data]).dt.strftime('%d/%m/%Y')
            
            st.dataframe(df_historico.rename(columns={
                'emissao': 'Data Emissão',
                'data_recebimento': 'Data Recebimento',
                'quantidade_comprada': 'Qtd.',
                'valor_unitario_compra': 'Custo Un. (R$)',
                'numero_nota_fiscal': 'NF'
            }))

#-------------------------------------------------------------------------------------------------------------------------------------------

# --- MÓDULO: PEDIDOS DE VENDA (Com Opção de Cupom Não Fiscal) ---

# Função para gerar o HTML do orçamento
def gerar_orcamento_html(cliente, itens, valor_total):
    linhas = ""
    for item in itens:
        linhas += f"""
        <tr>
            <td>{item['produto_nome']}</td>
            <td style="text-align:center">{item['quantidade']}</td>
            <td style="text-align:right">R$ {item['preco_unit']:.2f}</td>
            <td style="text-align:right">R$ {item['subtotal']:.2f}</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
            }}
            h2 {{
                text-align: center;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #000;
                padding: 8px;
            }}
            th {{
                background-color: #f0f0f0;
            }}
            .total {{
                text-align: right;
                font-size: 18px;
                margin-top: 15px;
                font-weight: bold;
            }}
            .info {{
                margin-top: 10px;
            }}
        </style>
    </head>
    <body>
        <h2>ORÇAMENTO</h2>

        <div class="info"><strong>Cliente:</strong> {cliente}</div>
        <div class="info"><strong>Data:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>

        <table>
            <thead>
                <tr>
                    <th>Produto</th>
                    <th>Qtd</th>
                    <th>Preço Unit.</th>
                    <th>Subtotal</th>
                </tr>
            </thead>
            <tbody>
                {linhas}
            </tbody>
        </table>

        <div class="total">Valor Total: R$ {valor_total:.2f}</div>

        <script>
            window.print();
        </script>
    </body>
    </html>
    """
    return html

if "vendas" not in st.session_state:
    st.session_state.vendas = []

elif choice == "Pedidos de Venda":
    from datetime import datetime

    st.header("Registro e Gestão de Pedidos de Venda")

    # ---------- SESSION STATE ----------
    if "vendas" not in st.session_state:
        st.session_state.vendas = []

    # ---------- DADOS ----------
    df_clientes = fetch_all("Clientes")
    df_produtos = fetch_all("Produtos")

    if df_clientes.empty or df_produtos.empty:
        st.warning("É necessário ter Clientes e Produtos cadastrados no DB.")
        st.stop()

    opcoes_clientes = dict(zip(df_clientes['nome'], df_clientes['cliente_id']))
    opcoes_produtos = dict(zip(df_produtos['descricao'], df_produtos['produto_id']))

    # ==========================================================
    # 🔍 CONSULTA DE ESTOQUE (ANTES DA VENDA)
    # ==========================================================
    with st.expander("🔍 Consulta Rápida de Estoque", expanded=False):

        termo_busca = st.text_input(
            "Digite o nome do produto para pesquisar",
            placeholder="Ex: Pastilha, Óleo, Corrente..."
        )

        if termo_busca:
            filtro = df_produtos[
                df_produtos['descricao'].str.contains(termo_busca, case=False, na=False)
            ]

            if filtro.empty:
                st.warning("Nenhum produto encontrado.")
            else:
                st.dataframe(
                    filtro[['descricao', 'marca', 'estoque_atual', 'preco_venda']].rename(columns={
                        'descricao': 'Produto',
                        'estoque_atual': 'Estoque Atual',
                        'preco_venda': 'Preço de Venda'
                    })
                )
        else:
            st.info("Digite algo para consultar o estoque.")

    # ==========================================================
    # 1️⃣ NOVO PEDIDO (MULTI PRODUTOS)
    # ==========================================================
    with st.expander("➕ Novo Pedido de Venda", expanded=True):

        cliente_selecionado = st.selectbox(
            "Cliente",
            options=list(opcoes_clientes.keys()),
            index=None,
            placeholder="Digite o nome do cliente"
        )
        
        forma_pagamento = st.selectbox(
            "Forma de Pagamento Prevista",
            ['Pix', 'Cartão de Crédito', 'Cartão de Débito', 'Dinheiro'],
            index=None,
            placeholder="Selecione a forma de pagamento"
        )

        st.subheader("Adicionar Produtos")

        # 🚨 ALTERAÇÃO: Usaremos 4 colunas agora
        col1, col2, col3, col4 = st.columns(4) 
        
        with col1:
            produto_item = st.selectbox(
                "Produto",
                options=list(opcoes_produtos.keys()),
                index=None,
                placeholder="Digite ou selecione o produto"
            )
        
        with col2:
            quantidade_item = st.number_input(
                "Quantidade",
                min_value=1,
                step=1,
                value=None,          # ← começa vazio
                key="qtd_item"
            )

        
        with col3:
            percentual_desconto = st.number_input(
                "Desconto (%)",
                min_value=0.0,
                max_value=100.0,
                value=None,         
                step=0.5,
                format="%.2f",
                key="desc_perc"
            )
        
        with col4:
            st.text("") # Espaço para alinhar o botão
            if st.button("➕ Adicionar Item"):
                produto_id = opcoes_produtos[produto_item]
                produto_data = df_produtos[df_produtos['produto_id'] == produto_id].iloc[0]

                estoque_atual = int(produto_data['estoque_atual'])
                preco_venda_original = float(produto_data['preco_venda'])
                
                # 🚨 CÁLCULO DO PREÇO COM DESCONTO
                fator_desconto = 1 - (percentual_desconto / 100)
                preco_unit_com_desconto = preco_venda_original * fator_desconto
                
                subtotal_item = preco_unit_com_desconto * int(quantidade_item)

                if quantidade_item > estoque_atual:
                    st.error(f"Estoque insuficiente! Disponível: {estoque_atual}")
                else:
                    # 🚨 ALTERAÇÃO: Adicionando dados do desconto no Session State para exibição
                    st.session_state.vendas.append({
                        "produto_id": produto_id,
                        "produto_nome": produto_item,
                        "quantidade": int(quantidade_item),
                        "preco_unit_original": preco_venda_original,     
                        "desconto_perc": percentual_desconto, 
                        "preco_unit": preco_unit_com_desconto,
                        "subtotal": subtotal_item                    
                    })
                    st.success("Item adicionado ao pedido.")

        # ---------- CARRINHO ----------
        if st.session_state.vendas:
            st.subheader("🛒 Itens do Pedido")

            df_itens = pd.DataFrame(st.session_state.vendas)
            
            # 🚨 ALTERAÇÃO: Exibição detalhada no carrinho
            st.dataframe(
                df_itens[[ 'produto_nome', 'quantidade', 'preco_unit_original', 'desconto_perc', 'preco_unit', 'subtotal' ]].rename(columns={
                    'produto_nome': 'Produto',
                    'quantidade': 'Qtd',
                    'preco_unit_original': 'Preço Original',
                    'desconto_perc': 'Desc. (%)',
                    'preco_unit': 'Preço Unit. c/ Desc.',
                    'subtotal': 'Subtotal'
                }),
                column_config={
                    'Preço Original': st.column_config.NumberColumn(format="R$ %.2f"),
                    'Preço Unit. c/ Desc.': st.column_config.NumberColumn(format="R$ %.2f"),
                    'Subtotal': st.column_config.NumberColumn(format="R$ %.2f"),
                    'Desc. (%)': st.column_config.NumberColumn(format="%.2f %%")
                }
            )

            valor_total = df_itens['subtotal'].sum()
            st.metric("💰 Valor Total (c/ Descontos)", f"R$ {valor_total:.2f}")

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                if st.button("🗑️ Limpar Itens"):
                    st.session_state.vendas = []
                    st.rerun()

            with col_b:
                if st.button("📄 Gerar Orçamento"):
                    html_orcamento = gerar_orcamento_html(
                        cliente=cliente_selecionado,
                        itens=st.session_state.vendas,
                        valor_total=valor_total
                    )
                    st.components.v1.html(html_orcamento, height=700, scrolling=True)

            with col_c:
                if st.button("📌 Registrar Pedido", type="primary"):
                    try:
                        conn = get_db_connection_new()
                        cursor = conn.cursor()

                        data_pedido = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        cliente_id = opcoes_clientes[cliente_selecionado]

                        # CORREÇÃO 1: Converter valor_total para float nativo
                        valor_total_float = float(valor_total)

                        cursor.execute("""
                            INSERT INTO Pedidos
                            (cliente_id, data_pedido, valor_total, status_pedido, forma_pagamento)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING pedido_id
                        """, (cliente_id, data_pedido, valor_total_float, 'Pendente', forma_pagamento))

                        # No PostgreSQL/Supabase, usamos fetchone para pegar o ID gerado
                        pedido_id = cursor.fetchone()[0]

                        for item in st.session_state.vendas:
                            # CORREÇÃO 2: Converter todos os valores do item para tipos nativos
                            qtd = int(item['quantidade'])
                            preco_unit = float(item['preco_unit'])
                            subtotal = float(item['subtotal'])
                            desconto_p = float(item['desconto_perc'])
                            prod_id = int(item['produto_id'])

                            cursor.execute("""
                                INSERT INTO Vendas
                                (pedido_id, produto_id, quantidade, preco_unitario, subtotal, desconto)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                pedido_id,
                                prod_id,
                                qtd,
                                preco_unit,
                                subtotal,
                                desconto_p
                            ))

                            # Atualização de estoque
                            cursor.execute("""
                                UPDATE Produtos
                                SET estoque_atual = estoque_atual - %s
                                WHERE produto_id = %s
                            """, (qtd, prod_id))

                        conn.commit()

                        st.success(f"Pedido #{pedido_id} registrado com sucesso!")
                        st.session_state.vendas = []
                        get_db_connection_cached.clear()
                        st.rerun()

                    except Exception as e:
                        if conn: conn.rollback()
                        st.error(f"Erro ao registrar pedido: {e}")
                    finally:
                        if cursor: cursor.close()
                        if conn: conn.close()

    st.markdown("---")

    # ==========================================================
    # 2️⃣ CONCLUIR OU CANCELAR PEDIDOS PENDENTES
    # ==========================================================
    st.subheader("✅ Processar Pedidos Pendentes")

    sql_pendentes = """
        SELECT P.pedido_id, C.nome AS cliente_nome, P.data_pedido, P.valor_total
        FROM Pedidos P
        LEFT JOIN Clientes C ON P.cliente_id = C.cliente_id
        WHERE P.status_pedido = 'Pendente'
        ORDER BY P.data_pedido
    """
    df_pendentes = pd.DataFrame(execute_query(sql_pendentes, fetch=True))

    if df_pendentes.empty:
        st.info("Nenhum pedido pendente.")
    else:
        st.dataframe(df_pendentes)

        pedidos_selecionados = st.multiselect(
            "Selecione os pedidos para concluir ou cancelar:",
            df_pendentes['pedido_id'].tolist()
        )

        imprimir_cupom = st.checkbox("Imprimir Cupom Não Fiscal (somente ao concluir)")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("✔️ Marcar como CONCLUÍDO", type="primary"):
                if pedidos_selecionados:
                    placeholders = ','.join(['%s'] * len(pedidos_selecionados))
                    execute_query(
                        f"UPDATE Pedidos SET status_pedido='Concluído' WHERE pedido_id IN ({placeholders})",
                        pedidos_selecionados
                    )
                    if imprimir_cupom:
                        for pid in pedidos_selecionados:
                            detalhes = get_order_details_for_coupon(pid)
                            st.markdown(generate_non_fiscal_coupon(pid, detalhes), unsafe_allow_html=True)
                    get_db_connection_cached.clear()
                    st.rerun()

        with col2:
            if st.button("❌ Marcar como CANCELADO"):
                if pedidos_selecionados:
                    placeholders = ','.join(['%s'] * len(pedidos_selecionados))
                    execute_query(
                        f"UPDATE Pedidos SET status_pedido='Cancelado' WHERE pedido_id IN ({placeholders})",
                        pedidos_selecionados
                    )
                    get_db_connection_cached.clear()
                    st.rerun()

#-------------------------------------------------------------------------------------------------------------------------------------------

# --- NOVO MÓDULO: DEVOLUÇÕES ---
elif choice == "Devoluções":
    st.header("🔄 Devolução de Produtos")

    # Busca pedidos concluídos para permitir devolução
    sql_pedidos = """
        SELECT P.pedido_id, C.nome AS cliente_nome, P.data_pedido 
        FROM Pedidos P
        JOIN Clientes C ON P.cliente_id = C.cliente_id
        WHERE P.status_pedido = 'Concluído'
        ORDER BY P.data_pedido DESC
    """
    df_pedidos_concluidos = pd.DataFrame(execute_query(sql_pedidos, fetch=True))

    if df_pedidos_concluidos.empty:
        st.info("Nenhum pedido concluído encontrado para realizar devoluções.")
    else:
        # 1. Seleção do Pedido
        pedido_opcoes = {f"Pedido #{row['pedido_id']} - {row['cliente_nome']}": row['pedido_id'] 
                         for _, row in df_pedidos_concluidos.iterrows()}
        
        selecao_pedido = st.selectbox("Selecione o Pedido da Devolução", list(pedido_opcoes.keys()), index=None)

        if selecao_pedido:
            pedido_id_sel = pedido_opcoes[selecao_pedido]
            
            # Busca itens daquele pedido
            sql_itens = """
                SELECT V.produto_id, Pr.descricao, V.quantidade, V.preco_unitario
                FROM Vendas V
                JOIN Produtos Pr ON V.produto_id = Pr.produto_id
                WHERE V.pedido_id = %s
            """
            df_itens_pedido = pd.DataFrame(execute_query(sql_itens, params=(pedido_id_sel,), fetch=True))

            with st.form("form_devolucao"):
                st.subheader("Detalhes da Devolução")
                
                col_dev1, col_dev2, col_dev3 = st.columns(3)
                
                # Seleção do Produto do Pedido
                produto_para_devolver = col_dev1.selectbox(
                    "Produto a ser devolvido", 
                    df_itens_pedido['descricao'].tolist()
                )
                
                item_selecionado = df_itens_pedido[df_itens_pedido['descricao'] == produto_para_devolver].iloc[0]
                
                qtd_devolver = col_dev2.number_input(
                    "Quantidade", 
                    min_value=1, 
                    max_value=int(item_selecionado['quantidade']),
                    step=1
                )

                # NOVO CAMPO: Data da Devolução
                data_devolucao_input = col_dev3.date_input("Data da Devolução", datetime.now())

                # --- PARÂMETROS DE ESTADO DO PRODUTO ---
                st.markdown("---")
                st.write("**Estado do Produto e Destinação**")
                
                estado_produto = st.radio(
                    "Qual o estado do produto devolvido?",
                    ["Novo / Perfeito Estado", "Avariado (Leve)", "Sucata / Danificado"],
                    help="Isso define se o item retornará ao estoque de venda."
                )

                retornar_estoque = st.checkbox(
                    "Apto a voltar ao estoque de vendas?", 
                    value=(estado_produto == "Novo / Perfeito Estado")
                )
                
                motivo = st.text_area("Motivo da Devolução")

                submitted_dev = st.form_submit_button("Confirmar Devolução", type="primary")

                if submitted_dev:
                    try:
                        conn = get_db_connection_new()
                        cursor = conn.cursor()

                        # 1. MAPEAMENTO DO ESTADO
                        mapa_estado = {
                            "Novo / Perfeito Estado": "Novo",
                            "Avariado (Leve)": "Avariado",
                            "Sucata / Danificado": "Sucata"
                        }
                        
                        estado_final = mapa_estado[estado_produto]

                        # 2. INSERE NA TABELA DE DEVOLUÇÕES (Incluindo data_devolucao)
                        sql_insert_dev = """
                            INSERT INTO Devolucoes 
                            (pedido_id, produto_id, quantidade, estado_produto, retornou_estoque, motivo, data_devolucao)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        params_dev = (
                            int(pedido_id_sel), 
                            int(item_selecionado['produto_id']), 
                            int(qtd_devolver), 
                            estado_final, 
                            bool(retornar_estoque), 
                            motivo,
                            data_devolucao_input # Passando a data selecionada
                        )
                        cursor.execute(sql_insert_dev, params_dev)

                        # 3. ATUALIZA O ESTOQUE SE NECESSÁRIO
                        if retornar_estoque:
                            sql_update_estoque = "UPDATE Produtos SET estoque_atual = estoque_atual + %s WHERE produto_id = %s"
                            cursor.execute(sql_update_estoque, (int(qtd_devolver), int(item_selecionado['produto_id'])))
                            msg_estoque = "✅ Estoque atualizado."
                        else:
                            msg_estoque = "⚠️ Item registrado mas NÃO retornou ao estoque."

                        conn.commit()
                        st.success(f"Devolução registrada com sucesso para o dia {data_devolucao_input.strftime('%d/%m/%Y')}! {msg_estoque}")
                        
                        get_db_connection_cached.clear()

                    except Exception as e:
                        if conn: conn.rollback()
                        st.error(f"Erro ao processar: {e}")
                    finally:
                        if cursor: cursor.close()
                        if conn: conn.close()

#-------------------------------------------------------------------------------------------------------------------------------------------

# --- NOVO MÓDULO: DESPESAS E FLUXO DE CAIXA ---
elif choice == "Despesas e Fluxo de Caixa":
    st.header("💸 Despesas e Fluxo de Caixa (Contas a Pagar)")
    
    with st.expander("➕ Registrar Nova Despesa/Pagamento"):
        with st.form("form_despesa"):
            st.subheader("Dados da Despesa")

            tipo = st.selectbox(
                "Tipo de Despesa",
                [
                    'Salário/Folha de Pagamento',
                    'Aluguel',
                    'Contas de Consumo (Água/Luz/Tel)',
                    'Impostos/Taxas',
                    'Manutenção/Reparos',
                    'Outros'
                ],
                index=None,
                placeholder="Selecione o tipo de despesa",
                key="tipo_d"
            )

            descricao = st.text_input(
                "Descrição Detalhada (Ex: Salário Ref. Outubro/2023)",
                key='desc_d'
            )

            col_d1, col_d2 = st.columns(2)
            valor = col_d1.number_input("Valor (R$)", min_value=0.01, format="%.2f", key='valor_d')
            data_vencimento = col_d2.date_input("Data de Vencimento", datetime.now().date(), key='data_venc_d')
            
            col_d3, col_d4 = st.columns(2)
            status = col_d3.selectbox(
                "Status de Pagamento",
                ['Pendente', 'Pago'],
                index=None,
                placeholder="Selecione o status",
                key="status_d"
            )

            if status == 'Pago':
                data_pagamento = col_d4.date_input("Data de Pagamento", datetime.now().date(), key='data_pgto_d')
            else:
                data_pagamento = None
            
            submitted = st.form_submit_button("Salvar Despesa")
            
            if submitted:
                if valor > 0 and tipo and status:
                    sql = """
                        INSERT INTO Despesas (tipo_despesa, descricao, valor, data_vencimento, status, data_pagamento)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    params = (tipo, descricao, float(valor), data_vencimento, status, data_pagamento)
                    if execute_query(sql, params):
                        st.success(f"Despesa '{tipo} - {descricao}' registrada com sucesso!")
                        get_db_connection_cached.clear()
                        st.rerun()
                    else:
                        st.error("Falha ao registrar despesa.")
                else:
                    st.error("Preencha Tipo, Status e informe um valor maior que zero.")

    st.markdown("---")
    
    st.subheader("Histórico e Controle de Despesas")

    df_despesas = fetch_all("Despesas")
    
    if not df_despesas.empty:
        # Formatação de datas (Postgres/Supabase retorna date/datetime normalmente compatível)
        df_despesas['data_vencimento'] = pd.to_datetime(df_despesas['data_vencimento']).dt.strftime('%d/%m/%Y')
        df_despesas['data_pagamento'] = df_despesas['data_pagamento'].apply(
            lambda x: pd.to_datetime(x).strftime('%d/%m/%Y') if pd.notna(x) and x != '' else '-'
        )

        st.dataframe(df_despesas.rename(columns={
            'tipo_despesa': 'Tipo',
            'descricao': 'Descrição',
            'valor': 'Valor (R$)',
            'data_vencimento': 'Vencimento',
            'status': 'Status',
            'data_pagamento': 'Data Pagamento'
        }))
    else:
        st.info("Nenhuma despesa registrada ainda.")
