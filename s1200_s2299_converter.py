import io
from typing import Dict, List

import pandas as pd
import streamlit as st
from lxml import etree

###############################################################################
# 🔎  DETECÇÃO RÁPIDA DE EVENTO ################################################
###############################################################################

def detect_event_code(xml_bytes: bytes) -> str:
    """Retorna o código do evento (ex.: 'S-1200', 'S-1010')."""
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return "UNKNOWN"
    if root.xpath(".//*[local-name()='evtTabRubrica']"):
        return "S-1010"
    if root.xpath(".//*[local-name()='evtRemun'] | .//*[local-name()='evt1200']"):
        return "S-1200"
    id_nodes = root.xpath(".//*[@Id][1]")
    if id_nodes:
        raw_id = id_nodes[0].get("Id", "")
        if "S-" in raw_id:
            return raw_id.split("S-")[-1].rjust(4, "0").join(["S-", ""])
    if root.xpath(".//*[local-name()='evtDeslig']"):
        return "S-2299"
    return "UNKNOWN"

###############################################################################
# ⛓️  CORE PARSERS (mesmos do commit anterior) #################################
###############################################################################

class BaseParser:
    @staticmethod
    def _txt(node: etree._Element, xpath: str) -> str:
        if node is None:
            return ""
        res = node.xpath(xpath)
        if not res:
            return ""
        val = res[0] if isinstance(res, list) else res
        if isinstance(val, str):
            return val.strip()
        if hasattr(val, "text") and val.text:
            return val.text.strip()
        return str(val).strip()

class Parser1010(BaseParser):
    """Extrai dados do evento S‑1010 em DataFrames."""

    # ---------------------------------------------------------------------
    # Column Maps – mantêm coerência com a camada de apresentação
    # ---------------------------------------------------------------------
    COL_EVT = {
        "id_evento": "ID Evento",
        "tp_amb": "Tipo Ambiente",
        "proc_emi": "Processo Emissão",
        "ver_proc": "Versão Processo",
        "tp_insc": "Tipo Inscrição",
        "nr_insc": "Nº Inscrição",
        "origem_arquivo": "Origem Arquivo",
    }
    COL_RUB = {
        "id_evento": "ID Evento",
        "acao": "Ação",
        "cod_rubr": "Código Rubrica",
        "ide_tab_rubr": "ID Tabela Rubrica",
        "ini_valid": "Início Vigência",
        "fim_valid": "Fim Vigência",
        "dsc_rubr": "Descrição Rubrica",
        "nat_rubr": "Natureza Rubrica",
        "tp_rubr": "Tipo Rubrica",
        "cod_inc_cp": "Incidência INSS",
        "cod_inc_irrf": "Incidência IRRF",
        "cod_inc_fgts": "Incidência FGTS",
        "origem_arquivo": "Origem Arquivo",
    }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @classmethod
    def parse(cls, xml_bytes: bytes, file_name: str) -> Dict[str, pd.DataFrame]:
        root = etree.fromstring(xml_bytes)
        evt_nodes = root.xpath(".//*[local-name()='evtTabRubrica']")
        if not evt_nodes:
            raise ValueError("XML sem <evtTabRubrica> (S-1010).")
        evt = evt_nodes[0]

        header = {
            "id_evento": evt.get("Id", ""),
            "tp_amb": cls._txt(evt, "string(.//*[local-name()='tpAmb'])"),
            "proc_emi": cls._txt(evt, "string(.//*[local-name()='procEmi'])"),
            "ver_proc": cls._txt(evt, "string(.//*[local-name()='verProc'])"),
            "tp_insc": cls._txt(evt, "string(.//*[local-name()='tpInsc'])"),
            "nr_insc": cls._txt(evt, "string(.//*[local-name()='nrInsc'])"),
            "origem_arquivo": file_name,
        }
        df_evt = pd.DataFrame([header]).rename(columns=cls.COL_EVT)

        rows_rub: List[dict] = []
        for action in ("inclusao", "alteracao", "exclusao"):
            for node in evt.xpath(f".//*[local-name()='{action}']"):
                ide = node.xpath(".//*[local-name()='ideRubrica']")[0]
                dados = node.xpath(".//*[local-name()='dadosRubrica']")
                dados = dados[0] if dados else None

                rows_rub.append(
                    {
                        "id_evento": header["id_evento"],
                        "acao": action,
                        "cod_rubr": cls._txt(ide, "string(.//*[local-name()='codRubr'])"),
                        "ide_tab_rubr": cls._txt(ide, "string(.//*[local-name()='ideTabRubr'])"),
                        "ini_valid": cls._txt(ide, "string(.//*[local-name()='iniValid'])"),
                        "fim_valid": cls._txt(ide, "string(.//*[local-name()='fimValid'])"),
                        "dsc_rubr": cls._txt(dados, "string(.//*[local-name()='dscRubr'])") if dados else "",
                        "nat_rubr": cls._txt(dados, "string(.//*[local-name()='natRubr'])") if dados else "",
                        "tp_rubr": cls._txt(dados, "string(.//*[local-name()='tpRubr'])") if dados else "",
                        "cod_inc_cp": cls._txt(dados, "string(.//*[local-name()='codIncCP'])") if dados else "",
                        "cod_inc_irrf": cls._txt(dados, "string(.//*[local-name()='codIncIRRF'])") if dados else "",
                        "cod_inc_fgts": cls._txt(dados, "string(.//*[local-name()='codIncFGTS'])") if dados else "",
                        "origem_arquivo": file_name,
                    }
                )

        df_rub = pd.DataFrame(rows_rub).rename(columns=cls.COL_RUB)

        # Conversões numéricas (exceto Natureza Rubrica)
        num_cols = [
            "Tipo Rubrica",
            "Incidência INSS",
            "Incidência IRRF",
            "Incidência FGTS",
        ]
        df_rub[num_cols] = df_rub[num_cols].apply(pd.to_numeric, errors="coerce")

        return {"CABECALHO_1010": df_evt, "RUBRICAS_1010": df_rub}

class Parser1200(BaseParser):
    """Extrai dados do evento S‑1200 em DataFrames."""

    COL_EVT = {
        "id_evento": "ID Evento",
        "per_apur": "Período Apuração",
        "ind_retif": "Ind Retificação",
        "tp_amb": "Tipo Ambiente",
        "cpf_trab": "CPF Trabalhador",
        "origem_arquivo": "Origem Arquivo",
    }
    COL_DMDEV = {
        "id_dmdev": "ID Demonstrativo",
        "id_evento": "ID Evento",
        "per_apur": "Período Apuração",
        "cpf_trab": "CPF Trabalhador",
        "cod_categ": "Código Categoria",
        "matricula": "Matrícula",
        "cod_lotacao": "Código Lotação",
        "origem_arquivo": "Origem Arquivo",
    }
    COL_RUB = {
        "id_dmdev": "ID Demonstrativo",
        "per_apur": "Período Apuração",
        "cpf_trab": "CPF Trabalhador",
        "matricula": "Matrícula",
        "cod_rubr": "Código Rubrica",
        "ide_tab_rubr": "ID Tabela Rubrica",
        "qtd_rubr": "Quantidade",
        "vr_rubr": "Valor Rubrica (R$)",
        "ind_apur_ir": "Ind Apur IR",
        "origem_arquivo": "Origem Arquivo",
    }

    @classmethod
    def parse(cls, xml_bytes: bytes, file_name: str) -> Dict[str, pd.DataFrame]:
        root = etree.fromstring(xml_bytes)
        evt_nodes = root.xpath(
            ".//*[local-name()='evtRemun'] | .//*[local-name()='evt1200']"
        )
        if not evt_nodes:
            raise ValueError("XML sem <evtRemun>/<evt1200> (S-1200).")
        evt = evt_nodes[0]

        header = {
            "id_evento": evt.get("Id", ""),
            "per_apur": cls._txt(evt, "string(.//*[local-name()='perApur'])"),
            "ind_retif": cls._txt(evt, "string(.//*[local-name()='indRetif'])"),
            "tp_amb": cls._txt(evt, "string(.//*[local-name()='tpAmb'])"),
            "cpf_trab": cls._txt(evt, "string(.//*[local-name()='cpfTrab'])").zfill(11),
            "origem_arquivo": file_name,
        }
        df_evt = pd.DataFrame([header]).rename(columns=cls.COL_EVT)

        rows_dm, rows_rb = [], []
        for dm in evt.xpath(".//*[local-name()='dmDev']"):
            id_dm = cls._txt(dm, "string(.//*[local-name()='ideDmDev'])")
            cod_cat = cls._txt(dm, "string(.//*[local-name()='codCateg'])")
            est = dm.xpath(".//*[local-name()='ideEstabLot']")[0]
            cod_lot = cls._txt(est, "string(./*[local-name()='codLotacao'])")
            matric = cls._txt(est, "string(.//*[local-name()='matricula'])").zfill(8)

            rows_dm.append({
                "id_dmdev": id_dm,
                "id_evento": header["id_evento"],
                "per_apur": header["per_apur"],
                "cpf_trab": header["cpf_trab"],
                "cod_categ": cod_cat,
                "matricula": matric,
                "cod_lotacao": cod_lot,
                "origem_arquivo": file_name,
            })

            for it in est.xpath(".//*[local-name()='itensRemun']"):
                rows_rb.append({
                    "id_dmdev": id_dm,
                    "per_apur": header["per_apur"],
                    "cpf_trab": header["cpf_trab"],
                    "matricula": matric,
                    "cod_rubr": cls._txt(it, "string(.//*[local-name()='codRubr'])"),
                    "ide_tab_rubr": cls._txt(it, "string(.//*[local-name()='ideTabRubr'])"),
                    "qtd_rubr": cls._txt(it, "string(.//*[local-name()='qtdRubr'])"),
                    "vr_rubr": cls._txt(it, "string(.//*[local-name()='vrRubr'])"),
                    "ind_apur_ir": cls._txt(it, "string(.//*[local-name()='indApurIR'])"),
                    "origem_arquivo": file_name,
                })

        df_dm = pd.DataFrame(rows_dm).rename(columns=cls.COL_DMDEV)
        df_rb = pd.DataFrame(rows_rb).rename(columns=cls.COL_RUB)

        # Conversões numéricas
        df_dm["Código Categoria"] = pd.to_numeric(df_dm["Código Categoria"], errors="coerce")
        df_rb[["Quantidade", "Valor Rubrica (R$)"]] = df_rb[["Quantidade", "Valor Rubrica (R$)"]].apply(pd.to_numeric, errors="coerce")

        return {
            "CABECALHO_1200": df_evt,
            "DEMONSTRATIVO_1200": df_dm,
            "RUBRICAS_1200": df_rb,
        }

class Parser2299(BaseParser):
    """Extrai dados do evento S-2299 em DataFrames."""

    COL_EVT = {
        "id_evento": "ID Evento",
        "dt_deslig": "Data Desligamento",
        "mtv_deslig": "Motivo Desligamento (código)",
        "ind_retif": "Ind Retificação",
        "tp_amb": "Tipo Ambiente",
        "cpf_trab": "CPF Trabalhador",
        "origem_arquivo": "Origem Arquivo",
    }
    COL_DMDEV = {
        "id_dmdev": "ID Demonstrativo",
        "id_evento": "ID Evento",
        "cpf_trab": "CPF Trabalhador",
        "cod_categ": "Código Categoria",
        "matricula": "Matrícula",
        "cod_lotacao": "Código Lotação",
        "origem_arquivo": "Origem Arquivo",
    }
    COL_RUB = {
        "id_dmdev": "ID Demonstrativo",
        "dt_deslig": "Data Desligamento",
        "cpf_trab": "CPF Trabalhador",
        "matricula": "Matrícula",
        "cod_rubr": "Código Rubrica",
        "ide_tab_rubr": "ID Tabela Rubrica",
        "qtd_rubr": "Quantidade",
        "vr_rubr": "Valor Rubrica (R$)",
        "ind_apur_ir": "Ind Apur IR",
        "origem_arquivo": "Origem Arquivo",
    }

    @classmethod
    def parse(cls, xml_bytes: bytes, file_name: str):
        root = etree.fromstring(xml_bytes)
        evt_nodes = root.xpath(".//*[local-name()='evtDeslig']")
        if not evt_nodes:
            raise ValueError("XML sem <evtDeslig> (S-2299).")
        evt = evt_nodes[0]

        header = {
            "id_evento": evt.get("Id", ""),
            "dt_deslig": cls._txt(evt, "string(.//*[local-name()='dtDeslig'])"),
            "mtv_deslig": cls._txt(evt, "string(.//*[local-name()='mtvDeslig'])"),
            "ind_retif": cls._txt(evt, "string(.//*[local-name()='indRetif'])"),
            "tp_amb": cls._txt(evt, "string(.//*[local-name()='tpAmb'])"),
            "cpf_trab": cls._txt(evt, "string(.//*[local-name()='cpfTrab'])").zfill(11),
            "origem_arquivo": file_name,
        }
        df_evt = pd.DataFrame([header]).rename(columns=cls.COL_EVT)

        rows_dm, rows_rb = [], []
        for dm in evt.xpath(".//*[local-name()='dmDev']"):
            id_dm = (
                dm.get("id")
                or dm.get("Id")
                or cls._txt(dm, "string(.//*[local-name()='ideDmDev'])")
            )
            cod_cat = cls._txt(dm, "string(.//*[local-name()='codCateg'])")
            est = dm.xpath(".//*[local-name()='ideEstabLot']")[0]
            cod_lot = cls._txt(est, "string(./*[local-name()='codLotacao'])")
            matric = cls._txt(est, "string(.//*[local-name()='matricula'])").zfill(8)

            rows_dm.append(
                {
                    "id_dmdev": id_dm,
                    "id_evento": header["id_evento"],
                    "cpf_trab": header["cpf_trab"],
                    "cod_categ": cod_cat,
                    "matricula": matric,
                    "cod_lotacao": cod_lot,
                    "origem_arquivo": file_name,
                }
            )

            for det in est.xpath(".//*[local-name()='detVerbas']"):
                rows_rb.append(
                    {
                        "id_dmdev": id_dm,
                        "dt_deslig": header["dt_deslig"],
                        "cpf_trab": header["cpf_trab"],
                        "matricula": matric,
                        "cod_rubr": cls._txt(det, "string(.//*[local-name()='codRubr'])"),
                        "ide_tab_rubr": cls._txt(det, "string(.//*[local-name()='ideTabRubr'])"),
                        "qtd_rubr": cls._txt(det, "string(.//*[local-name()='qtdRubr'])"),
                        "vr_rubr": cls._txt(det, "string(.//*[local-name()='vrRubr'])"),
                        "ind_apur_ir": cls._txt(det, "string(.//*[local-name()='indApurIR'])"),
                        "origem_arquivo": file_name,
                    }
                )

        df_dm = pd.DataFrame(rows_dm).rename(columns=cls.COL_DMDEV)
        df_rb = pd.DataFrame(rows_rb).rename(columns=cls.COL_RUB)

        # Conversões numéricas
        df_dm["Código Categoria"] = pd.to_numeric(df_dm["Código Categoria"], errors="coerce")
        df_rb[["Quantidade", "Valor Rubrica (R$)"]] = df_rb[
            ["Quantidade", "Valor Rubrica (R$)"]
        ].apply(pd.to_numeric, errors="coerce")

        return {
            "CABECALHO_2299": df_evt,
            "DEMONSTRATIVO_2299": df_dm,
            "RUBRICAS_2299": df_rb,
        }

###############################################################################
# 🔗 FUNÇÃO DE JOIN COM CONVERSÃO DE DATAS ####################################
###############################################################################

def harmonizar_rubricas_2299(df_rb_2299: pd.DataFrame) -> pd.DataFrame:
    """
    Converte 'Data Desligamento' (dt_deslig) → 'Período Apuração'
    no formato YYYY-MM, alinha ordem de colunas para poder dar concat.
    """
    df = df_rb_2299.copy()

    # cria coluna 'Período Apuração' (yyyy-mm) a partir de dt_deslig
    df["Período Apuração"] = pd.to_datetime(
        df["Data Desligamento"], errors="coerce"
    ).dt.strftime("%Y-%m")

    # garante que a nova coluna fique na mesma posição da do S-1200
    col_order_target = [
        "ID Demonstrativo",
        "Período Apuração",
        "CPF Trabalhador",
        "Matrícula",
        "Código Rubrica",
        "ID Tabela Rubrica",
        "Quantidade",
        "Valor Rubrica (R$)",
        "Ind Apur IR",
        "Origem Arquivo",
        # colunas exclusivas do S-2299 que queremos preservar
        "Data Desligamento",
    ]
    # adiciona colunas faltantes como vazias
    for c in col_order_target:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[col_order_target]  # reordena

    return df

def join_rubricas(df_rb: pd.DataFrame, df_rub: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece RUBRICAS_1200 (df_rb) com campos do RUBRICAS_1010 (df_rub)
    utilizando somente o 'Código Rubrica' como chave.
    """
    # cópias defensivas
    df_rb  = df_rb.copy()
    df_rub = df_rub.copy()

    # merge simples
    merged = df_rb.merge(
        df_rub,
        how="left",
        on="Código Rubrica",
        suffixes=("", "_1010"),
    )

    # seleciona colunas extras vindas do 1010
    extra_cols = [
        "Início Vigência", "Fim Vigência", "Descrição Rubrica", "Natureza Rubrica",
        "Tipo Rubrica", "Incidência INSS", "Incidência IRRF", "Incidência FGTS",
        "Origem Arquivo_1010",
    ]

    # concatena lado a lado
    return pd.concat(
        [df_rb.reset_index(drop=True), merged[extra_cols].reset_index(drop=True)],
        axis=1
    )

###############################################################################
# 🖥️  STREAMLIT UI (idem, com join chamado) ##################################
###############################################################################

st.set_page_config(page_title="Planilhamento de XMLs – 1010, 2299 & 1200", layout="wide")
st.title("Planilhamento de XMLs – S-1010, S-2299 e S-1200 (eSocial)")

st.markdown(
    """
    **Fluxo sugerido:**
    1. Carregue os XMLs do **S-1010**
    2. Carregue os XMLs do **S-2299**
    3. Carregue os XMLs do **S-1200** 
    4. Baixe o Excel **consolidado** com todas as tabelas.

    ⚠️ Arquivos de eventos **diferentes** de *S‑1010*, *S-2299* e *S‑1200* serão automaticamente
    ignorados, e uma mensagem identificará o código recebido.
    """
)

###############################################################################
# Passo 1 – S-1010 ############################################################
###############################################################################

st.subheader("Passo 1 – Tabela de Rubricas (S-1010)")
files_1010 = st.file_uploader(
    "Selecione os arquivos XML do S-1010", type=["xml"], accept_multiple_files=True, key="s1010"
)

parsed_1010: Dict[str, List[pd.DataFrame]] = {}
if files_1010:
    tmp: Dict[str, List[pd.DataFrame]] = {"CABECALHO_1010": [], "RUBRICAS_1010": []}
    for f in files_1010:
        event_code = detect_event_code(f.getvalue())
        if event_code != "S-1010":
            st.warning(f"{f.name}: evento {event_code} não suportado neste passo; arquivo ignorado.")
            continue
        try:
            res = Parser1010.parse(f.getvalue(), f.name)
            for k in tmp:
                tmp[k].append(res[k])
        except Exception as e:
            st.error(f"Erro ao processar {f.name}: {e}")
    parsed_1010 = {k: pd.concat(v, ignore_index=True) for k, v in tmp.items() if v}

    for name, df in parsed_1010.items():
        with st.expander(f"{name} – {len(df):,} linhas"):
            st.dataframe(df, use_container_width=True)

###############################################################################
# Passo 2 – S-2299 ############################################################
###############################################################################

st.subheader("Passo 2 – Desligamentos (S-2299)")

files_2299 = st.file_uploader(
    "Selecione os arquivos XML do S-2299",
    type=["xml"],
    accept_multiple_files=True,
    key="s2299",
)

parsed_2299 = {}
if files_2299:
    tmp3 = {"CABECALHO_2299": [], "DEMONSTRATIVO_2299": [], "RUBRICAS_2299": []}
    for f in files_2299:
        event_code = detect_event_code(f.getvalue())
        if event_code != "S-2299":
            st.warning(f"{f.name}: evento {event_code} não suportado neste passo; arquivo ignorado.")
            continue
        try:
            res = Parser2299.parse(f.getvalue(), f.name)
            for k in tmp3:
                tmp3[k].append(res[k])
        except Exception as e:
            st.error(f"Erro ao processar {f.name}: {e}")
    parsed_2299 = {k: pd.concat(v, ignore_index=True) for k, v in tmp3.items() if v}

    for name, df in parsed_2299.items():
        with st.expander(f"{name} – {len(df):,} linhas"):
            st.dataframe(df, use_container_width=True)

###############################################################################
# Passo 3 – S-1200 ############################################################
###############################################################################

st.subheader("Passo 3 – Remunerações (S-1200)")

files_1200 = st.file_uploader(
    "Selecione os arquivos XML do S-1200", type=["xml"], accept_multiple_files=True, key="s1200"
)

parsed_1200: Dict[str, List[pd.DataFrame]] = {}
if files_1200:
    tmp2: Dict[str, List[pd.DataFrame]] = {
        "CABECALHO_1200": [],
        "DEMONSTRATIVO_1200": [],
        "RUBRICAS_1200": [],
    }
    for f in files_1200:
        event_code = detect_event_code(f.getvalue())
        if event_code != "S-1200":
            st.warning(f"{f.name}: evento {event_code} não suportado neste passo; arquivo ignorado.")
            continue
        try:
            res = Parser1200.parse(f.getvalue(), f.name)
            for k in tmp2:
                tmp2[k].append(res[k])
        except Exception as e:
            st.error(f"Erro ao processar {f.name}: {e}")
    parsed_1200 = {k: pd.concat(v, ignore_index=True) for k, v in tmp2.items() if v}

    for name, df in parsed_1200.items():
        with st.expander(f"{name} – {len(df):,} linhas"):
            st.dataframe(df, use_container_width=True)

######################################################################
# RUBRICAS 1200 + 2299  ➜  ENRIQUECE COM S-1010
######################################################################
if parsed_1010 and parsed_1200 and parsed_2299:
    # 1) Harmoniza e empilha rubricas de 1200 + 2299
    df_1200_rb = parsed_1200["RUBRICAS_1200"].copy()
    df_2299_rb = harmonizar_rubricas_2299(parsed_2299["RUBRICAS_2299"])

    df_rubricas_unific = pd.concat([df_1200_rb, df_2299_rb], ignore_index=True)

    # 2) Enriquecer com dados do S-1010
    df_joined = join_rubricas(
        df_rubricas_unific,
        parsed_1010["RUBRICAS_1010"].copy(),
    )

    # 3) Exibe resultado
    with st.expander(f"RUBRICAS_1200_2299_ENRIQUECIDO – {len(df_joined):,} linhas", expanded=False):
        st.dataframe(df_joined, use_container_width=True)

    # 4) Salva para download
    parsed_1200["RUBRICAS_1200_2299_ENRIQUECIDO"] = df_joined

###############################################################################
# Excel Consolidado ###########################################################
###############################################################################

if parsed_1010 and parsed_2299 and parsed_1200:
    buf_all = io.BytesIO()
    with pd.ExcelWriter(buf_all, engine="openpyxl") as wr:

        # ➤ Tabelas do S-1010
        for name, df in parsed_1010.items():
            df.to_excel(wr, index=False, sheet_name=name[:31])

        # ➤ Tabelas do S-2299
        for name, df in parsed_2299.items():
            df.to_excel(wr, index=False, sheet_name=name[:31])

        # ➤ Rubricas unificadas (1200 + 2299) já enriquecidas
        if "RUBRICAS_1200_2299_ENRIQUECIDO" in parsed_1200:
            parsed_1200["RUBRICAS_1200_2299_ENRIQUECIDO"].to_excel(
                wr,
                index=False,
                sheet_name="RUBRICAS_1200_2299"  # ≤31 chars
            )

        # ➤ Demais tabelas “puras” do S-1200
        for name, df in parsed_1200.items():
            if name == "RUBRICAS_1200_2299_ENRIQUECIDO":
                continue  # já foi salva acima
            df.to_excel(wr, index=False, sheet_name=name[:31])

    buf_all.seek(0)

    st.download_button(
        "📥 Baixar Consolidado (1010-2299-1200)",
        buf_all.getvalue(),
        "s1010_s2299_s1200.xlsx",
    )
else:
    st.info("Envie arquivos válidos dos eventos 1010, 2299 e 1200 para liberar o consolidado.")