# =========================
# ATUALIZACAO:
# - Mantidas bases:
#   Email
#   Nome + CPF/CNPJ
#   Telefones + Melhor Contato (Top 7 + status/obs do Tel1)
#   Acordos (Promessa/Em Acordo) P/A
#   CPC por Periodo (data inicio / data fim na UI)
#   Sem Historico (ultimos 30 dias)
#   Base Recentes
#   Nunca Contatados
#   Quebras Rejeitadas
#
# - ADICIONADA:
#   Garantias (bens_tb) - qtd garantias + marca/modelo + placas
#
# - Base CPC por Periodo: usuario informa Data Inicio e Data Fim (YYYY-MM-DD). Se vazio, roda sem filtro de data.
# =========================

import os
import re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd
import mysql.connector


CRED_FILE_PATH = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials_Copia.txt"

CARTEIRAS = [
    ("517 Itapeva Autos", 517),
    ("518 DivZero", 518),
    ("519 Cedidas", 519),
]

TEL_LIMIT_FIXO = 7


# =========================
# Credenciais (arquivo)
# =========================
def parse_credentials_from_file(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de credenciais nao encontrado: {path}")

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    def pick(key: str):
        m = re.search(rf"^{key}\s*=\s*(.+?)\s*$", content, re.MULTILINE)
        if not m:
            return None
        raw = m.group(1).split("#", 1)[0].strip()
        return raw.strip().strip('"').strip("'")

    host = pick("GECOBI_HOST")
    user = pick("GECOBI_USER")
    passwd = pick("GECOBI_PASS")
    db = pick("GECOBI_DB")
    port = pick("GECOBI_PORT")

    missing = [k for k, v in {
        "GECOBI_HOST": host,
        "GECOBI_USER": user,
        "GECOBI_PASS": passwd,
        "GECOBI_DB": db,
        "GECOBI_PORT": port
    }.items() if not v]

    if missing:
        raise ValueError("Chaves nao encontradas no arquivo de credenciais: " + ", ".join(missing))

    try:
        port_int = int(port)
    except ValueError:
        raise ValueError(f"GECOBI_PORT invalido no arquivo: {port!r}")

    return {
        "host": host,
        "user": user,
        "password": passwd,
        "database": db,
        "port": port_int
    }


# =========================
# SQLs simples
# =========================
SQL_EMAIL = """
SELECT nomecli, cpfcnpj, email
FROM cadastros_tb
WHERE cod_cli IN ({cod_cli})
  AND stcli <> 'INA';
"""

SQL_NOME_CPF = """
SELECT nomecli, cpfcnpj
FROM cadastros_tb
WHERE cod_cli IN ({cod_cli})
  AND stcli <> 'INA';
"""

# =========================
# Base 1: Telefones + Melhor contato (Top 7) + status/obs do telefone 1
# =========================
SQL_TELEFONES_MELHOR_CONTATO = r"""
WITH telefones AS (
    SELECT
        cad.cod_cad,
        cad.nomecli AS nome,
        cad.cpfcnpj AS cpf,
        cad.nmcont,
        cad.cod_cli,
        cad.infoad AS portfolio,
        CONCAT(tel.dddfone, tel.telefone) AS telefone,
        tel.status,
        tel.obs,
        ROW_NUMBER() OVER (
            PARTITION BY cad.cod_cad
            ORDER BY FIELD(tel.status, 2, 4, 5, 6, 1, 0), tel.status
        ) AS num
    FROM cadastros_tb cad
    JOIN fones_tb tel
        ON tel.cod_cad = cad.cod_cad
    WHERE cad.cod_cli IN ({cod_cli})
      AND cad.stcli <> 'INA'
      AND (tel.status IN (2,4,5,6,1)
           OR (tel.obs NOT LIKE '%Descon%' AND tel.obs NOT LIKE '%incorret%'))
      AND CONCAT(tel.dddfone, tel.telefone) NOT REGEXP '([0-9])\\1{{5}}'
      AND LENGTH(CONCAT(tel.dddfone, tel.telefone)) >= 8
      AND CONCAT(tel.dddfone, tel.telefone) NOT LIKE '%X%'
),
filtrados AS (
    SELECT * FROM telefones WHERE num <= {tel_limit}
)
SELECT
    cod_cad, nome, cpf, nmcont, cod_cli, portfolio,

    MAX(CASE WHEN num = 1 THEN telefone END) AS Telefone1,
    MAX(CASE WHEN num = 1 THEN status   END) AS StatusTelefone1,
    MAX(CASE WHEN num = 1 THEN obs      END) AS ObsTelefone1,

    MAX(CASE WHEN num = 2 THEN telefone END) AS Telefone2,
    MAX(CASE WHEN num = 3 THEN telefone END) AS Telefone3,
    MAX(CASE WHEN num = 4 THEN telefone END) AS Telefone4,
    MAX(CASE WHEN num = 5 THEN telefone END) AS Telefone5,
    MAX(CASE WHEN num = 6 THEN telefone END) AS Telefone6,
    MAX(CASE WHEN num = 7 THEN telefone END) AS Telefone7
FROM filtrados
GROUP BY cod_cad, nome, cpf, nmcont, cod_cli, portfolio;
"""

# =========================
# Base 5: Acordos em Promessa / Em Acordo (P/A) - ultimo acordo por cliente
# =========================
SQL_ACORDOS_PA = r"""
WITH ranked AS (
    SELECT
        a.nmcont,
        a.cod_cli,
        a.cod_aco,
        a.data_cad,
        a.vlr_aco,
        a.qtd_p_aco,
        a.staco,
        ROW_NUMBER() OVER (PARTITION BY a.nmcont, a.cod_cli ORDER BY a.cod_aco DESC) AS rn
    FROM acordos_tb a
    WHERE a.cod_cli IN ({cod_cli})
      AND a.data_cad >= '2025-07-01'
)
SELECT
    nmcont,
    cod_cli,
    cod_aco,
    DATE_FORMAT(data_cad, '%d-%m-%Y') AS DataCriacaoGecobi,
    vlr_aco AS UltimoValorAcordado,
    qtd_p_aco,
    CASE WHEN qtd_p_aco = 1 THEN 'AVISTA' ELSE 'PARCELADO' END AS TipoAcordo,
    staco
FROM ranked
WHERE rn = 1
  AND staco IN ('P','A')
ORDER BY cod_aco DESC;
"""

# =========================
# Base 8: CPC por periodo (data inicio / data fim)
# =========================
SQL_CPC_PERIODO = r"""
SELECT
    cad.cod_cad,
    cad.nomecli AS nome,
    cad.cpfcnpj AS cpf,
    cad.nmcont,
    MAX(his.data_at) AS dt_ultimo_cpc
FROM cadastros_tb cad
JOIN hist_tb his
    ON his.cod_cli = cad.cod_cad
JOIN stcob_tb st
    ON st.st = his.ocorr
WHERE cad.cod_cli IN ({cod_cli})
  AND cad.stcli <> 'INA'
  AND st.bsc LIKE '%CPC%'
  {dt_ini_filter}
  {dt_fim_filter}
GROUP BY cad.cod_cad, cad.nomecli, cad.cpfcnpj, cad.nmcont;
"""

# =========================
# Base 9: Sem historico ultimos 30 dias
# =========================
SQL_SEM_HIST_30D = r"""
SELECT
    cad.cod_cad,
    cad.nomecli AS nome,
    cad.cpfcnpj AS cpf,
    cad.nmcont,
    cad.cod_cli,
    cad.infoad AS portfolio
FROM cadastros_tb cad
WHERE cad.cod_cli IN ({cod_cli})
  AND cad.stcli <> 'INA'
  AND cad.cod_cad NOT IN (
      SELECT h.cod_cli
      FROM hist_tb h
      WHERE h.data_at >= NOW() - INTERVAL 30 DAY
        AND h.cod_usu <> '999'
      GROUP BY h.cod_cli
  );
"""

# =========================
# Base Recentes (AJUSTADO p/ multi-carteira: IN)
# =========================
SQL_RECENTES = r"""
WITH
cpc_ultimo AS (
    SELECT
        st.bsc,
        cad.nmcont,
        MAX(his.data_at) AS dt_ultimo_cpc
    FROM cadastros_tb cad
    JOIN hist_tb his
        ON his.cod_cli = cad.cod_cad
    JOIN stcob_tb st
        ON st.st = his.ocorr
    WHERE cad.cod_cli IN ({cod_cli})
      AND cad.stcli <> 'INA'
    GROUP BY cad.nmcont
),
acordos_ranked AS (
    SELECT
        a.nmcont,
        a.cod_aco,
        a.data_aco,
        a.data_cad,
        a.vlr_aco,
        a.qtd_p_aco,
        a.staco,
        ROW_NUMBER() OVER (
            PARTITION BY a.nmcont
            ORDER BY a.cod_aco DESC
        ) AS rn_aco
    FROM acordos_tb a
    WHERE a.cod_cli IN ({cod_cli})
      AND a.data_cad >= '2025-07-01'
),
acordos_pagos AS (
    SELECT nmcont
    FROM acordos_ranked
    WHERE rn_aco = 1
      AND staco IN ('P','G','A')
),
acordos_ultimos AS (
    SELECT
        aco.nmcont,
        aco.vlr_aco AS UltimoValorAcordado,
        CASE
            WHEN aco.qtd_p_aco = 1 THEN 'AVISTA'
            WHEN aco.qtd_p_aco > 1 THEN 'PARCELADO'
            ELSE NULL
        END AS TipoAcordo,
        DATE_FORMAT(aco.data_cad, '%d-%m-%Y') AS DataCriacaoGecobi,
        CASE aco.staco
            WHEN 'A' THEN 'Em Acordo'
            WHEN 'E' THEN 'Exceção Rejeitada'
            WHEN 'G' THEN 'Pago'
            WHEN 'Q' THEN 'Quebrado'
            WHEN 'P' THEN 'Em Promessa'
            ELSE 'Sem Dados'
        END AS StatusUltimoAcordo,
        aco.staco
    FROM acordos_ranked aco
    WHERE aco.rn_aco = 1
      AND aco.staco IN ('P','G','A','Q','E')
),
valores AS (
    SELECT
        recc.nmcont,
        recc.cod_cli,
        GROUP_CONCAT(DISTINCT rec.fat_parc ORDER BY rec.fat_parc SEPARATOR ' || ') AS Contratos,
        GROUP_CONCAT(DISTINCT recc.char_5 ORDER BY recc.char_5 SEPARATOR ' || ') AS TipoProduto
    FROM rec_comp_tb recc
    LEFT JOIN receber_tb rec
        ON rec.nmcont = recc.nmcont
       AND rec.cod_cli = recc.cod_cli
    WHERE recc.cod_cli IN ({cod_cli})
      AND rec.fat_parc NOT LIKE '%ENTRADA%'
      AND recc.nmcont NOT IN (SELECT nmcont FROM acordos_pagos)
    GROUP BY recc.nmcont, recc.cod_cli
),
bens AS (
    SELECT
        ben.nmcont,
        ben.cod_cli,
        CONCAT(ben.marca, ' - ', ben.modelo) AS MarcaModelo,
        ben.placa,
        ben.cor,
        CONCAT(ben.anofab, '/', ben.anomodelo) AS AnoFabModelo,
        COUNT(DISTINCT ben.chassi) AS QtdGarantiasUnicas
    FROM bens_tb ben
    WHERE ben.cod_cli IN ({cod_cli})
    GROUP BY ben.nmcont, ben.cod_cli
),
telefones AS (
    SELECT
        cad.cod_cad AS cod_cad,
        cad.nomecli AS nome,
        cad.cpfcnpj AS cpf,
        cad.nmcont AS nmcont,
        cad.cod_cli AS cod_cli,
        MAX(recc.int_2) AS BindingID,
        DATE_FORMAT(nascto, '%d-%m-%Y') AS DataNascimento,
        cad.infoad AS Portfolio,
        CONCAT(dddfone,telefone) AS telefones,
        ROW_NUMBER() OVER (
            PARTITION BY tel.cod_cad
            ORDER BY FIELD(tel.status, 2, 4, 5, 6, 1, 0), tel.status
        ) AS num
    FROM cadastros_tb cad
    JOIN fones_tb tel
        ON tel.cod_cad = cad.cod_cad
    LEFT JOIN rec_comp_tb recc
        ON recc.nmcont = cad.nmcont
       AND cad.cod_cli = recc.cod_cli
    WHERE cad.cod_cli IN ({cod_cli})
      AND cad.data_cad = cad.data_arq
      AND cad.data_cad >= (curdate() - interval 2 month)
      AND CONCAT(dddfone, telefone) NOT REGEXP '([0-9])\\1{{5}}'
      AND cad.stcli <> 'INA'
    GROUP BY
        cad.cod_cad,
        cad.nomecli,
        cad.cpfcnpj,
        cad.nmcont,
        cad.cod_cli,
        nascto,
        cad.infoad,
        dddfone,
        telefone,
        tel.status
),
telefones_filtrados AS (
    SELECT * FROM telefones WHERE num <= {tel_limit}
),
telefones_final AS (
    SELECT
        cod_cad,
        nome,
        cpf,
        nmcont,
        cod_cli,
        BindingID,
        DataNascimento,
        Portfolio,
        MAX(CASE WHEN num = 1 THEN telefones END) AS Telefone1,
        MAX(CASE WHEN num = 2 THEN telefones END) AS Telefone2,
        MAX(CASE WHEN num = 3 THEN telefones END) AS Telefone3,
        MAX(CASE WHEN num = 4 THEN telefones END) AS Telefone4,
        MAX(CASE WHEN num = 5 THEN telefones END) AS Telefone5,
        MAX(CASE WHEN num = 6 THEN telefones END) AS Telefone6,
        MAX(CASE WHEN num = 7 THEN telefones END) AS Telefone7
    FROM telefones_filtrados
    GROUP BY
        cod_cad, nome, cpf, nmcont, cod_cli, BindingID, DataNascimento, Portfolio
)
SELECT
    t.cod_cad,
    t.nome,
    t.cpf,
    t.BindingID,
    t.DataNascimento,
    t.Portfolio,
    t.Telefone1, t.Telefone2, t.Telefone3, t.Telefone4, t.Telefone5, t.Telefone6, t.Telefone7,
    b.MarcaModelo,
    b.placa,
    b.cor,
    b.AnoFabModelo,
    b.QtdGarantiasUnicas,
    v.Contratos,
    v.TipoProduto,
    a.UltimoValorAcordado,
    a.TipoAcordo,
    a.DataCriacaoGecobi,
    a.StatusUltimoAcordo
FROM telefones_final t
LEFT JOIN bens b
    ON t.nmcont = b.nmcont
   AND t.cod_cli = b.cod_cli
JOIN valores v
    ON t.nmcont = v.nmcont
   AND t.cod_cli = v.cod_cli
LEFT JOIN acordos_ultimos a
    ON t.nmcont = a.nmcont
LEFT JOIN cpc_ultimo cpc
    ON t.nmcont = cpc.nmcont
WHERE t.cod_cli IN ({cod_cli});
"""

# =========================
# Nunca Contatados
# =========================

SQL_NUNCA = r"""
WITH
cpc_ultimo AS (
    SELECT
        cad.nmcont,
        MAX(his.data_at) AS dt_ultimo_cpc
    FROM cadastros_tb cad
    JOIN hist_tb his ON his.cod_cli = cad.cod_cad
    JOIN stcob_tb st ON st.st = his.ocorr
    WHERE cad.cod_cli IN ({cod_cli})
      AND cad.stcli <> 'INA'
      AND st.bsc LIKE '%%CPC%%'
    GROUP BY cad.nmcont
),
acordos_ranked AS (
    SELECT
        a.nmcont,
        a.cod_aco,
        a.data_aco,
        a.data_cad,
        a.vlr_aco,
        a.qtd_p_aco,
        a.staco,
        ROW_NUMBER() OVER (PARTITION BY a.nmcont ORDER BY a.cod_aco DESC) AS rn_aco
    FROM acordos_tb a
    WHERE a.cod_cli IN ({cod_cli})
      AND a.data_cad >= '2025-07-01'
),
acordos_pagos AS (
    SELECT nmcont
    FROM acordos_ranked
    WHERE rn_aco = 1
      AND staco IN ('P','G','A')
),
acordos_ultimos AS (
    SELECT
        aco.nmcont,
        aco.vlr_aco AS UltimoValorAcordado,
        CASE
            WHEN aco.qtd_p_aco = 1 THEN 'AVISTA'
            WHEN aco.qtd_p_aco > 1 THEN 'PARCELADO'
            ELSE NULL
        END AS TipoAcordo,
        DATE_FORMAT(aco.data_cad, '%d-%m-%Y') AS DataCriacaoGecobi,
        CASE aco.staco
            WHEN 'A' THEN 'Em Acordo'
            WHEN 'E' THEN 'Exceção Rejeitada'
            WHEN 'G' THEN 'Pago'
            WHEN 'Q' THEN 'Quebrado'
            WHEN 'P' THEN 'Em Promessa'
            ELSE 'Sem Dados'
        END AS StatusUltimoAcordo,
        aco.staco
    FROM acordos_ranked aco
    WHERE aco.rn_aco = 1
      AND aco.staco IN ('Q','E','P','G','A')
),
valores AS (
    SELECT
        recc.nmcont,
        recc.cod_cli,
        GROUP_CONCAT(DISTINCT rec.fat_parc ORDER BY rec.fat_parc SEPARATOR ' || ') AS Contratos,
        GROUP_CONCAT(DISTINCT recc.char_5 ORDER BY recc.char_5 SEPARATOR ' || ') AS TipoProduto
    FROM rec_comp_tb recc
    LEFT JOIN receber_tb rec
        ON rec.nmcont = recc.nmcont
       AND rec.cod_cli = recc.cod_cli
    WHERE recc.cod_cli IN ({cod_cli})
      AND rec.fat_parc NOT LIKE '%%ENTRADA%%'
      AND recc.nmcont NOT IN (SELECT nmcont FROM acordos_pagos)
    GROUP BY recc.nmcont, recc.cod_cli
),
bens AS (
    SELECT
        ben.nmcont,
        ben.cod_cli,
        CONCAT(ben.marca, ' - ', ben.modelo) AS MarcaModelo,
        ben.placa,
        ben.cor,
        CONCAT(ben.anofab, '/', ben.anomodelo) AS AnoFabModelo,
        COUNT(DISTINCT ben.chassi) AS QtdGarantiasUnicas
    FROM bens_tb ben
    WHERE ben.cod_cli IN ({cod_cli})
    GROUP BY ben.nmcont, ben.cod_cli
),
telefones AS (
    SELECT
        cad.cod_cad AS cod_cad,
        cad.nomecli AS nome,
        cad.cpfcnpj AS cpf,
        cad.nmcont AS nmcont,
        cad.cod_cli AS cod_cli,
        MAX(recc.int_2) AS BindingID,
        DATE_FORMAT(nascto, '%d-%m-%Y') AS DataNascimento,
        cad.infoad AS Portfolio,
        CONCAT(dddfone,telefone) AS telefones,
        ROW_NUMBER() OVER (
            PARTITION BY tel.cod_cad
            ORDER BY FIELD(tel.status, 2, 4, 5, 6, 1, 0), tel.status
        ) AS num
    FROM cadastros_tb cad
    JOIN fones_tb tel ON tel.cod_cad = cad.cod_cad
    LEFT JOIN rec_comp_tb recc
        ON recc.nmcont = cad.nmcont
       AND cad.cod_cli = recc.cod_cli
    WHERE cad.cod_cli IN ({cod_cli})
      AND CONCAT(dddfone, telefone) NOT REGEXP '([0-9])\\1{{5}}'
      AND cad.cod_cad NOT IN(
            SELECT h.cod_cli
            FROM hist_tb h
            LEFT JOIN stcob_tb s ON s.st = h.ocorr
            WHERE h.cod_cli = cad.cod_cad
              AND h.data_at >= CURDATE() - INTERVAL 30 DAY
              AND (s.bsc NOT LIKE '%%sistema%%' OR s.bsc NOT LIKE '' OR s.bsc IS NOT NULL)
              AND h.cod_usu <> '999'
            GROUP BY h.cod_cli
      )
      AND (tel.status IN (2, 4, 5, 6, 1)
           OR (tel.obs NOT LIKE '%%Descon%%' AND tel.obs NOT LIKE '%%incorret%%'))
      AND cad.stcli <> 'INA'
      AND LENGTH(CONCAT(dddfone, telefone)) >= 8
      AND CONCAT(dddfone, telefone) NOT LIKE '%%X%%'
    GROUP BY cad.cod_cad, cad.nomecli, cad.cpfcnpj, cad.nmcont, cad.cod_cli,
             nascto, cad.infoad, dddfone, telefone, tel.status
),
telefones_filtrados AS (
    SELECT * FROM telefones WHERE num <= {tel_limit}
),
telefones_final AS (
    SELECT
        cod_cad, nome, cpf, nmcont, cod_cli, bindingid, datanascimento, portfolio,
        MAX(CASE WHEN num = 1 THEN telefones END) AS Telefone1,
        MAX(CASE WHEN num = 2 THEN telefones END) AS Telefone2,
        MAX(CASE WHEN num = 3 THEN telefones END) AS Telefone3,
        MAX(CASE WHEN num = 4 THEN telefones END) AS Telefone4,
        MAX(CASE WHEN num = 5 THEN telefones END) AS Telefone5,
        MAX(CASE WHEN num = 6 THEN telefones END) AS Telefone6,
        MAX(CASE WHEN num = 7 THEN telefones END) AS Telefone7
    FROM telefones_filtrados
    GROUP BY cod_cad, nome, cpf, nmcont, cod_cli, bindingid, datanascimento, portfolio
)
SELECT
    t.cod_cad,
    t.nome,
    t.cpf,
    t.bindingid,
    t.datanascimento,
    t.portfolio,
    t.Telefone1, t.Telefone2, t.Telefone3, t.Telefone4, t.Telefone5, t.Telefone6, t.Telefone7,
    b.MarcaModelo,
    b.placa,
    b.cor,
    b.AnoFabModelo,
    b.QtdGarantiasUnicas,
    v.Contratos,
    v.TipoProduto,
    a.UltimoValorAcordado,
    a.TipoAcordo,
    a.DataCriacaoGecobi,
    a.StatusUltimoAcordo
FROM telefones_final t
LEFT JOIN bens b
    ON t.nmcont = b.nmcont AND t.cod_cli = b.cod_cli
JOIN valores v
    ON t.nmcont = v.nmcont AND t.cod_cli = v.cod_cli
LEFT JOIN acordos_ultimos a
    ON t.nmcont = a.nmcont
LEFT JOIN cpc_ultimo cpc
    ON t.nmcont = cpc.nmcont
WHERE t.cod_cli IN ({cod_cli});
"""

# =========================
# Quebras Rejeitadas
# =========================
SQL_QUEBRAS_REJEITADAS = r"""
WITH
cpc_ultimo AS (
    SELECT
        st.bsc,
        cad.nmcont,
        MAX(his.data_at) AS dt_ultimo_cpc
    FROM cadastros_tb cad
    JOIN hist_tb his ON his.cod_cli = cad.cod_cad
    JOIN stcob_tb st ON st.st = his.ocorr
    WHERE cad.cod_cli IN ({cod_cli})
      AND cad.stcli <> 'INA'
      {infoad_filter}
    GROUP BY cad.nmcont
),
acordos_ranked AS (
    SELECT
        a.nmcont,
        a.cod_aco,
        a.data_aco,
        a.data_cad,
        a.vlr_aco,
        a.qtd_p_aco,
        a.staco,
        ROW_NUMBER() OVER (PARTITION BY a.nmcont ORDER BY a.cod_aco DESC) AS rn_aco
    FROM acordos_tb a
    WHERE a.cod_cli IN ({cod_cli})
      AND a.data_cad >= '2025-07-01'
),
acordos_pagos AS (
    SELECT nmcont
    FROM acordos_ranked
    WHERE rn_aco = 1
      AND staco IN ('P','G','A')
),
acordos_ultimos AS (
    SELECT
        aco.nmcont,
        aco.vlr_aco AS UltimoValorAcordado,
        CASE
            WHEN aco.qtd_p_aco = 1 THEN 'AVISTA'
            WHEN aco.qtd_p_aco > 1 THEN 'PARCELADO'
            ELSE NULL
        END AS TipoAcordo,
        DATE_FORMAT(aco.data_cad, '%d-%m-%Y') AS DataCriacaoGecobi,
        CASE aco.staco
            WHEN 'A' THEN 'Em Acordo'
            WHEN 'E' THEN 'Exceção Rejeitada'
            WHEN 'G' THEN 'Pago'
            WHEN 'Q' THEN 'Quebrado'
            WHEN 'P' THEN 'Em Promessa'
            ELSE 'Sem Dados'
        END AS StatusUltimoAcordo,
        aco.staco
    FROM acordos_ranked aco
    WHERE aco.rn_aco = 1
      AND aco.staco IN ('Q','E','P','G','A')
),
acordos_ex AS (
    SELECT aco.nmcont
    FROM acordos_ranked aco
    WHERE aco.rn_aco = 1
      AND aco.staco IN ('Q','E')
),
valores AS (
    SELECT
        recc.nmcont,
        recc.cod_cli,
        GROUP_CONCAT(DISTINCT rec.fat_parc ORDER BY rec.fat_parc SEPARATOR ' || ') AS Contratos,
        GROUP_CONCAT(DISTINCT recc.char_5 ORDER BY recc.char_5 SEPARATOR ' || ') AS TipoProduto
    FROM rec_comp_tb recc
    LEFT JOIN receber_tb rec
        ON rec.nmcont = recc.nmcont
       AND rec.cod_cli = recc.cod_cli
    WHERE recc.cod_cli IN ({cod_cli})
      AND rec.fat_parc NOT LIKE '%ENTRADA%'
      AND recc.nmcont NOT IN (SELECT nmcont FROM acordos_pagos)
    GROUP BY recc.nmcont, recc.cod_cli
    {vlrparc_having}
),
bens AS (
    SELECT
        ben.nmcont,
        ben.cod_cli,
        CONCAT(ben.marca, ' - ', ben.modelo) AS MarcaModelo,
        ben.placa,
        ben.cor,
        CONCAT(ben.anofab, '/', ben.anomodelo) AS AnoFabModelo,
        COUNT(DISTINCT ben.chassi) AS QtdGarantiasUnicas
    FROM bens_tb ben
    WHERE ben.cod_cli IN ({cod_cli})
    GROUP BY ben.nmcont, ben.cod_cli
),
telefones AS (
    SELECT
        cad.cod_cad AS cod_cad,
        cad.nomecli AS nome,
        cad.cpfcnpj AS cpf,
        cad.nmcont AS nmcont,
        cad.cod_cli AS cod_cli,
        MAX(recc.int_2) AS BindingID,
        DATE_FORMAT(nascto, '%d-%m-%Y') AS DataNascimento,
        cad.infoad AS Portfolio,
        CONCAT(dddfone,telefone) AS telefones,
        ROW_NUMBER() OVER (
            PARTITION BY tel.cod_cad
            ORDER BY FIELD(tel.status, 2, 4, 5, 6, 1, 0), tel.status
        ) AS num
    FROM cadastros_tb cad
    JOIN fones_tb tel
        ON tel.cod_cad = cad.cod_cad
    LEFT JOIN rec_comp_tb recc
        ON recc.nmcont = cad.nmcont AND cad.cod_cli = recc.cod_cli
    WHERE cad.cod_cli IN ({cod_cli})
      AND (tel.status IN (2, 4, 5, 6, 1)
           OR (tel.obs NOT LIKE '%Descon%' AND tel.obs NOT LIKE '%incorret%'))
      AND cad.stcli <> 'INA'
      AND CONCAT(dddfone, telefone) NOT REGEXP '([0-9])\\1{{5}}'
      AND LENGTH(CONCAT(dddfone, telefone)) >= 8
      AND CONCAT(dddfone, telefone) NOT LIKE '%X%'
      {infoad_filter}
    GROUP BY cad.cod_cad, cad.nomecli, cad.cpfcnpj, cad.nmcont, cad.cod_cli,
             nascto, cad.infoad, dddfone, telefone, tel.status
),
telefones_filtrados AS (
    SELECT * FROM telefones WHERE num <= {tel_limit}
),
telefones_final AS (
    SELECT
        cod_cad, nome, cpf, nmcont, cod_cli, bindingid, datanascimento, portfolio,
        MAX(CASE WHEN num = 1 THEN telefones END) AS Telefone1,
        MAX(CASE WHEN num = 2 THEN telefones END) AS Telefone2,
        MAX(CASE WHEN num = 3 THEN telefones END) AS Telefone3,
        MAX(CASE WHEN num = 4 THEN telefones END) AS Telefone4,
        MAX(CASE WHEN num = 5 THEN telefones END) AS Telefone5,
        MAX(CASE WHEN num = 6 THEN telefones END) AS Telefone6,
        MAX(CASE WHEN num = 7 THEN telefones END) AS Telefone7
    FROM telefones_filtrados
    GROUP BY cod_cad, nome, cpf, nmcont, cod_cli, bindingid, datanascimento, portfolio
)
SELECT
    t.cod_cad,
    t.nome,
    t.cpf,
    t.bindingid,
    t.datanascimento,
    t.portfolio,
    t.Telefone1, t.Telefone2, t.Telefone3, t.Telefone4, t.Telefone5, t.Telefone6, t.Telefone7,
    b.MarcaModelo,
    b.placa,
    b.cor,
    b.AnoFabModelo,
    b.QtdGarantiasUnicas,
    v.Contratos,
    v.TipoProduto,
    a.UltimoValorAcordado,
    a.TipoAcordo,
    a.DataCriacaoGecobi,
    a.StatusUltimoAcordo
FROM telefones_final t
LEFT JOIN bens b
    ON t.nmcont = b.nmcont AND t.cod_cli = b.cod_cli
JOIN valores v
    ON t.nmcont = v.nmcont AND t.cod_cli = v.cod_cli
LEFT JOIN acordos_ultimos a
    ON t.nmcont = a.nmcont
JOIN acordos_ex ex
    ON t.nmcont = ex.nmcont
LEFT JOIN cpc_ultimo cpc
    ON t.nmcont = cpc.nmcont
WHERE t.cod_cli IN ({cod_cli});
"""

# =========================
# Garantias (bens_tb)
# =========================
SQL_GARANTIAS_BENS = r"""
SELECT
    cad.cod_cad,
    cad.nomecli AS nome,
    cad.cpfcnpj AS cpf,
    cad.nmcont,
    cad.cod_cli,
    COUNT(DISTINCT ben.chassi) AS QtdGarantiasUnicas,
    GROUP_CONCAT(DISTINCT CONCAT(ben.marca,' - ',ben.modelo) ORDER BY ben.marca, ben.modelo SEPARATOR ' || ') AS MarcaModelo,
    GROUP_CONCAT(DISTINCT ben.placa ORDER BY ben.placa SEPARATOR ' || ') AS Placas
FROM cadastros_tb cad
JOIN bens_tb ben
    ON ben.nmcont = cad.nmcont
   AND ben.cod_cli = cad.cod_cli
WHERE cad.cod_cli IN ({cod_cli})
  AND cad.stcli <> 'INA'
GROUP BY cad.cod_cad, cad.nomecli, cad.cpfcnpj, cad.nmcont, cad.cod_cli
ORDER BY QtdGarantiasUnicas DESC;
"""


# =========================
# Runner SQL (IN multi-carteiras)
# =========================
def _is_valid_ymd(s: str) -> bool:
    if not s:
        return False
    return re.fullmatch(r"\d{4}-\d{2}-\d{2}", s) is not None


def build_sql_and_params(sql_template: str, carteiras: list[int], extra: dict | None = None) -> tuple[str, list]:
    if not carteiras:
        raise ValueError("Nenhuma carteira selecionada.")

    extra = extra or {}
    in_placeholders = ", ".join(["%s"] * len(carteiras))

    # Monta o SQL final
    sql = sql_template.format(
        cod_cli=in_placeholders,
        tel_limit=TEL_LIMIT_FIXO,
        dt_ini_filter=extra.get("dt_ini_filter", ""),
        dt_fim_filter=extra.get("dt_fim_filter", ""),
        infoad_filter=extra.get("infoad_filter", ""),
        vlrparc_having=extra.get("vlrparc_having", ""),
    )

    date_params = extra.get("_date_params", []) or []

    # Conta quantos %s existem no SQL FINAL
    total_placeholders = sql.count("%s")

    # Quantos %s sobram para carteiras (tirando os de data)
    carteira_placeholders = total_placeholders - len(date_params)
    if carteira_placeholders < 0:
        raise ValueError("SQL possui menos placeholders do que parâmetros de data.")

    # Preenche parâmetros de carteiras ciclando (garante quantidade exata)
    params = [carteiras[i % len(carteiras)] for i in range(carteira_placeholders)]
    params += date_params

    return sql, params


def run_query(sql_template: str, carteiras: list[int], extra: dict | None = None) -> pd.DataFrame:
    creds = parse_credentials_from_file(CRED_FILE_PATH)
    sql, params = build_sql_and_params(sql_template, carteiras, extra=extra)

    conn = mysql.connector.connect(**creds)
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


# =========================
# Mapa de consultas (UI)
# =========================
QUERIES = {
    "Email (nome, CPF/CNPJ, email)": (SQL_EMAIL, "base_email.xlsx", "Email"),
    "Nome + CPF/CNPJ": (SQL_NOME_CPF, "base_nome_cpf.xlsx", "NomeCPF"),
    "Telefones + Melhor Contato (Top 7)": (SQL_TELEFONES_MELHOR_CONTATO, "base_telefones_melhor_contato.xlsx", "TelefonesTop7"),
    "Acordos (Promessa/Em Acordo) P/A": (SQL_ACORDOS_PA, "base_acordos_PA.xlsx", "AcordosPA"),
    "CPC por Periodo (datas)": (SQL_CPC_PERIODO, "base_cpc_periodo.xlsx", "CPCPeriodo"),
    "Sem Historico (ultimos 30 dias)": (SQL_SEM_HIST_30D, "base_sem_hist_30d.xlsx", "SemHist30d"),
    "Garantias (bens_tb)": (SQL_GARANTIAS_BENS, "base_garantias_bens.xlsx", "Garantias"),

    "Quebras Rejeitadas": (SQL_QUEBRAS_REJEITADAS, "base_quebras_rejeitadas.xlsx", "QuebrasRejeitadas"),
    "Nunca Contatados": (SQL_NUNCA, "base_nunca.xlsx", "Nunca"),
    "Base Recentes": (SQL_RECENTES, "base_recentes.xlsx", "Recentes"),
}


# =========================
# UI
# =========================
class App(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Gerador de Bases - Itapeva")
        self.geometry("980x620")
        self.minsize(940, 580)

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("Title.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Sub.TLabel", font=("Segoe UI", 9))
        style.configure("Card.TLabelframe", padding=10)
        style.configure("Card.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"), padding=8)
        style.configure("TButton", padding=6)
        style.configure("TCombobox", padding=4)

        header = ttk.Frame(self, padding=(14, 12))
        header.pack(fill="x")

        ttk.Label(header, text="Gerador de Bases", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Dúvidas ou não achou a base que precisa? Entre em Contato: juridico577@oliveiraeantunes.com.br",
            style="Sub.TLabel"
        ).pack(anchor="w", pady=(2, 0))

        main = ttk.Frame(self, padding=(14, 0, 14, 14))
        main.pack(fill="both", expand=True)

        top = ttk.Frame(main)
        top.pack(fill="x", pady=(8, 10))

        lf_cart = ttk.LabelFrame(top, text="Carteiras", style="Card.TLabelframe")
        lf_cart.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.carteira_vars = []
        for label, code in CARTEIRAS:
            var = tk.BooleanVar(value=False)
            self.carteira_vars.append((var, code))
            ttk.Checkbutton(lf_cart, text=label, variable=var).pack(anchor="w", pady=2)

        lf_q = ttk.LabelFrame(top, text="Consulta", style="Card.TLabelframe")
        lf_q.pack(side="left", fill="x", expand=True)

        self.query_var = tk.StringVar(value=list(QUERIES.keys())[0])
        self.combo = ttk.Combobox(
            lf_q,
            textvariable=self.query_var,
            values=list(QUERIES.keys()),
            state="readonly",
            width=44
        )
        self.combo.pack(anchor="w", pady=(2, 8))
        self.combo.bind("<<ComboboxSelected>>", self._on_query_change)

        ttk.Label(lf_q, text=f"Telefone fixo: top {TEL_LIMIT_FIXO}", style="Sub.TLabel").pack(anchor="w")

        lf_params = ttk.LabelFrame(main, text="Parametros (apenas para 'CPC por Periodo')", style="Card.TLabelframe")
        lf_params.pack(fill="x", pady=(0, 10))

        row = ttk.Frame(lf_params)
        row.pack(fill="x")

        ttk.Label(row, text="Data Inicio (YYYY-MM-DD):").pack(side="left", padx=(0, 6))
        self.dt_ini_var = tk.StringVar(value="")
        self.dt_ini_entry = ttk.Entry(row, textvariable=self.dt_ini_var, width=14)
        self.dt_ini_entry.pack(side="left", padx=(0, 14))

        ttk.Label(row, text="Data Fim (YYYY-MM-DD):").pack(side="left", padx=(0, 6))
        self.dt_fim_var = tk.StringVar(value="")
        self.dt_fim_entry = ttk.Entry(row, textvariable=self.dt_fim_var, width=14)
        self.dt_fim_entry.pack(side="left", padx=(0, 14))

        ttk.Label(row, text="(vazio = sem filtro)", style="Sub.TLabel").pack(side="left")

        self._set_date_fields_enabled(False)

        actions = ttk.Frame(main)
        actions.pack(fill="x", pady=(0, 10))

        self.btn_generate = ttk.Button(actions, text="Gerar Excel", style="Primary.TButton", command=self.gerar_excel)
        self.btn_generate.pack(side="left")

        self.btn_clear = ttk.Button(actions, text="Limpar selecao", command=self.limpar)
        self.btn_clear.pack(side="left", padx=10)

        lf_log = ttk.LabelFrame(main, text="Status / Log", style="Card.TLabelframe")
        lf_log.pack(fill="both", expand=True)

        log_frame = ttk.Frame(lf_log)
        log_frame.pack(fill="both", expand=True)

        self.log = tk.Text(log_frame, height=14, wrap="word")
        self.log.pack(side="left", fill="both", expand=True)

        scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log.yview)
        scroll.pack(side="right", fill="y")
        self.log.configure(yscrollcommand=scroll.set)

        self.escrever_log("Pronto. Selecione a(s) carteira(s), escolha a consulta e clique em 'Gerar Excel'.")

    def escrever_log(self, msg: str):
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.update_idletasks()

    def limpar(self):
        for var, _ in self.carteira_vars:
            var.set(False)
        self.query_var.set(list(QUERIES.keys())[0])
        self.dt_ini_var.set("")
        self.dt_fim_var.set("")
        self._set_date_fields_enabled(False)
        self.escrever_log("Selecoes limpas.")

    def _set_date_fields_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        self.dt_ini_entry.configure(state=state)
        self.dt_fim_entry.configure(state=state)

    def _on_query_change(self, _evt=None):
        self._set_date_fields_enabled(self.query_var.get() == "CPC por Periodo (datas)")

    def _build_extra_for_selected_query(self, query_name: str) -> dict:
        if query_name != "CPC por Periodo (datas)":
            return {}

        dt_ini = self.dt_ini_var.get().strip()
        dt_fim = self.dt_fim_var.get().strip()

        extra = {"_date_params": []}

        if dt_ini:
            if not _is_valid_ymd(dt_ini):
                raise ValueError("Data Inicio invalida. Use YYYY-MM-DD.")
            extra["dt_ini_filter"] = "AND his.data_at >= %s"
            extra["_date_params"].append(dt_ini + " 00:00:00")
        else:
            extra["dt_ini_filter"] = ""

        if dt_fim:
            if not _is_valid_ymd(dt_fim):
                raise ValueError("Data Fim invalida. Use YYYY-MM-DD.")
            extra["dt_fim_filter"] = "AND his.data_at <= %s"
            extra["_date_params"].append(dt_fim + " 23:59:59")
        else:
            extra["dt_fim_filter"] = ""

        return extra

    def gerar_excel(self):
        carteiras = [c for v, c in self.carteira_vars if v.get()]
        if not carteiras:
            messagebox.showwarning("Atencao", "Selecione ao menos uma carteira (517/518/519).")
            return

        query_name = self.query_var.get()
        sql_template, default_filename, sheet_name = QUERIES[query_name]

        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=default_filename,
            filetypes=[("Excel", "*.xlsx")],
            title="Salvar Excel"
        )
        if not path:
            self.escrever_log("Geracao cancelada.")
            return

        self.btn_generate.config(state="disabled")
        try:
            extra = self._build_extra_for_selected_query(query_name)

            self.escrever_log("-" * 60)
            self.escrever_log(f"Carteiras: {carteiras}")
            self.escrever_log(f"Consulta: {query_name}")
            if query_name == "CPC por Periodo (datas)":
                self.escrever_log(f"Data Inicio: {self.dt_ini_var.get().strip() or '(vazio)'}")
                self.escrever_log(f"Data Fim: {self.dt_fim_var.get().strip() or '(vazio)'}")
            self.escrever_log(f"Credenciais: {CRED_FILE_PATH}")
            self.escrever_log("Executando...")

            df = run_query(sql_template, carteiras, extra=extra)

            self.escrever_log(f"Linhas retornadas: {len(df)}")
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)

            self.escrever_log(f"✅ Excel gerado: {path}")
            messagebox.showinfo("Sucesso", f"Excel gerado com sucesso!\n\n{path}")

        except FileNotFoundError as e:
            self.escrever_log(f"❌ {e}")
            messagebox.showerror("Credenciais nao encontradas", str(e))

        except mysql.connector.Error as e:
            self.escrever_log(f"❌ Erro de banco: {e}")
            messagebox.showerror("Erro no banco", f"Falha ao conectar/consultar:\n{e}")

        except Exception as e:
            self.escrever_log(f"❌ Erro: {e}")
            messagebox.showerror("Erro", str(e))

        finally:
            self.btn_generate.config(state="normal")


if __name__ == "__main__":
    App().mainloop()
