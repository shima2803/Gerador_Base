# Gerador de Bases - Itapeva (Tkinter + MariaDB)

Aplicativo em Python (Tkinter) para gerar bases de cobrança a partir do banco MariaDB, escolhendo **carteiras** e **consultas**, e exportando o resultado para **Excel (.xlsx)** com log de execução na tela.

---

## ✅ O que o sistema faz

- Lê credenciais do banco a partir de um arquivo (`SA_Credencials_Copia.txt`)
- Permite selecionar 1 ou mais carteiras (517/518/519)
- Executa consultas SQL pré-definidas (Email, Nome+CPF, Telefones Top 7, Acordos P/A, CPC por período, Sem histórico 30 dias, Garantias etc.)
- Exporta o retorno para Excel com nome padrão sugerido
- Exibe status/log no próprio aplicativo

---

## 📌 Requisitos

- Python 3.10+ (recomendado)
- Bibliotecas:
  - `pandas`
  - `mysql-connector-python`
  - `openpyxl`

Instalação:
```bash
pip install pandas mysql-connector-python openpyxl
```
# 🔐 Credenciais do Banco
O sistema usa o arquivo:

Copy code
\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials_Copia.txt
Esse arquivo deve conter as chaves:

GECOBI_HOST

GECOBI_USER

GECOBI_PASS

GECOBI_DB

GECOBI_PORT

# ▶️ Como executar
No terminal, dentro da pasta do script:

bash
Copy code
python gerador_base.py
## 🧩 Como usar (passo a passo)
Abra o sistema

Marque a(s) carteira(s) desejada(s)

Selecione a consulta no combo

Se for CPC por Periodo, informe Data Início/Fim (YYYY-MM-DD) ou deixe vazio

Clique em Gerar Excel

Escolha onde salvar o arquivo

# 📄 Consultas disponíveis
Email (nome, CPF/CNPJ, email)

Nome + CPF/CNPJ

Telefones + Melhor Contato (Top 7)

Acordos (Promessa/Em Acordo) P/A

CPC por Periodo (datas)

Sem Historico (ultimos 30 dias)

Garantias (bens_tb)

Bases grandes (ex.: Quebras Rejeitadas, Nunca Contatados, Recentes — conforme SQLs disponíveis no código)
--- 
# ⚠️ Observações importantes
Algumas bases usam SQL com WHERE cod_cli = {cod_cli} e podem exigir apenas 1 carteira marcada.

Caso ocorra erro de parâmetros (Not enough parameters), normalmente é porque a SQL tem IN ({cod_cli}) repetido e o builder precisa multiplicar corretamente os parâmetros.

O aviso do Pandas (pandas only supports SQLAlchemy...) é apenas warning e não impede a execução.

# 📩 Suporte

Caso você tenha alguma dúvida, ou não ache a base que você precisa, entre em contato com:
juridico577@oliveiraeantunes.com.br

ps: isso é um beta teste, porem caso não tenha nenhum erro, será a versao final
