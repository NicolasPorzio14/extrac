# app.py
# Streamlit ETL: TXT (copiado de PDF con OCR) -> Excel (Movimientos + Auditoría)
#
# Ejecutar:
#   pip install streamlit pandas openpyxl
#   streamlit run app.py
#
# Qué hace:
# - Subís un .txt con el contenido copiado del PDF (OCR ya hecho).
# - Parseo: fecha, concepto, referencia, importe, saldo.
# - Débito/Crédito se clasifica por delta de saldo (regla principal).
# - Auditoría: delta vs importe, filas sin importe, sin saldo, etc.
# - Descargás un .xlsx con 2 hojas: Movimientos y Auditoría.

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st


# =========================================================
# Config UI
# =========================================================
st.set_page_config(page_title="ETL Extracto → Excel", page_icon="📄", layout="wide")
st.title("📄 ETL Extracto bancario (TXT OCR) → Excel")
st.caption("Subí el TXT copiado del PDF (OCR ya hecho). Genera Excel con auditoría lógica por SALDO.")


# =========================================================
# Regex / parsing helpers
# =========================================================
DATE_RE = re.compile(r"^(?P<d>\d{2}/\d{2}/\d{2})\b")

# Token monetario (termina en 2 decimales) + soporta OCR tipo 172.291.54
MONEY_TOKEN_RE = re.compile(
    r"""
    ^[+-]?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})$   # 1.234,56 / 1,234.56 / 172.291.54
    |^[+-]?\d+(?:[.,]\d{2})$                   # 1234,56 / 1234.56
    """,
    re.VERBOSE,
)

INT_TOKEN_RE = re.compile(r"^\d+$")


def _clean_ocr_weird_chars(s: str) -> str:
    # Normaliza caracteres raros típicos del OCR
    repl = {
        "Dв": "DB",
        "DВ": "DB",
        "DBв": "DB",
        "ARBА": "ARBA",  # A cirílica
        "С": "C",
        "О": "O",
        "–": "-",
        "−": "-",
    }
    for k, v in repl.items():
        s = s.replace(k, v)

    # OCR típico: "115.5o" -> "115.50"
    s = re.sub(r"(\d)[oO](\b|$)", r"\g<1>0\2", s)
    return s


def parse_ar_number(token: str) -> Optional[float]:
    """
    Convierte números estilo AR/mixto a float.
    Soporta:
      - 1.234,56 / 1234,56
      - 1,234.56 / 1234.56
      - 1.648.32 (OCR: miles con puntos + decimal con punto)
      - -155.642.50 (OCR)
    """
    t = token.strip()
    if not t:
        return None
    if not re.fullmatch(r"[-+]?\d[\d\.,]*", t):
        return None

    sign = -1.0 if t.startswith("-") else 1.0
    t2 = t.lstrip("+-")

    # OCR: múltiples puntos y NO coma => si último grupo tiene 2 dígitos, último punto es decimal
    if "." in t2 and t2.count(".") >= 2 and "," not in t2:
        parts = t2.split(".")
        if len(parts[-1]) == 2 and all(p.isdigit() for p in parts):
            t2 = "".join(parts[:-1]) + "." + parts[-1]
        else:
            t2 = t2.replace(".", "")

    # Múltiples comas y NO punto (raro): si último grupo tiene 2 dígitos => decimal
    if "," in t2 and t2.count(",") >= 2 and "." not in t2:
        parts = t2.split(",")
        if len(parts[-1]) == 2 and all(p.isdigit() for p in parts):
            t2 = "".join(parts[:-1]) + "." + parts[-1]
        else:
            t2 = t2.replace(",", "")

    # Mixto coma+punto: decide por el ÚLTIMO separador
    if "," in t2 and "." in t2:
        last_comma = t2.rfind(",")
        last_dot = t2.rfind(".")
        if last_comma > last_dot:
            # decimal = ','
            t2 = t2.replace(".", "")
            t2 = t2.replace(",", ".")
        else:
            # decimal = '.'
            t2 = t2.replace(",", "")
    elif "," in t2:
        # decimal = ','
        t2 = t2.replace(".", "")
        t2 = t2.replace(",", ".")

    try:
        return sign * float(t2)
    except ValueError:
        return None


def parse_date(line: str) -> Optional[datetime]:
    m = DATE_RE.match(line.strip())
    if not m:
        return None
    try:
        return datetime.strptime(m.group("d"), "%d/%m/%y")
    except ValueError:
        return None


def is_header_noise(line: str) -> bool:
    l = line.strip().upper()
    if not l:
        return True
    noisy = [
        "DETALLE DE MOVIMIENTOS",
        "RESUMEN DE CUENTA",
        "CUENTA CORRIENTE",
        "FECHA CONCEPTO",
        "DÉBITO CRÉDITO SALDO",
        "DEBITO CREDITO SALDO",
        "SUBTOTAL",
        "I.V.A. RESPONSABLE",
        "I.V.A.",
        "C.U.I.T",
        "NRO COMERCIO",
        "MARCA:",
        "CBU:",
        "BENEF:",
        "PÁGINA",
        "PAGINA",
    ]
    return any(x in l for x in noisy)


def _looks_like_page_number_only(line: str) -> bool:
    return bool(re.fullmatch(r"\d{1,4}", line.strip()))


def _is_balance_only_line(line: str) -> Optional[float]:
    """
    Si la línea es solamente un saldo (ej: '170,005.52' o '169.452,37'),
    devuelve el float. Si no, None.
    """
    s = line.strip().replace("$", "").strip()
    parts = s.split()

    # Caso: "13 93,416.95" (contador de línea/página)
    if len(parts) == 2 and _looks_like_page_number_only(parts[0]) and MONEY_TOKEN_RE.fullmatch(parts[1]):
        s = parts[1]

    if MONEY_TOKEN_RE.fullmatch(s):
        return parse_ar_number(s)
    return None


def _extract_tail_money_and_ref(line: str) -> Tuple[Optional[float], Optional[float], Optional[str], str]:
    """
    Extrae desde el final:
      - saldo = último token monetario
      - importe = token monetario anterior al saldo
      - referencia = entero largo (8-14 dígitos) cercano al final
    Devuelve: (importe, saldo, referencia, texto_restante)
    """
    parts = line.strip().split()
    if not parts:
        return None, None, None, ""

    money_vals: List[Tuple[int, float]] = []
    ref: Optional[str] = None

    # Recorremos desde la derecha, cortando cuando salimos de la "cola"
    for i in range(len(parts) - 1, -1, -1):
        tok = parts[i]

        if MONEY_TOKEN_RE.fullmatch(tok):
            val = parse_ar_number(tok)
            if val is not None:
                money_vals.append((i, val))
            continue

        if INT_TOKEN_RE.fullmatch(tok) and 8 <= len(tok) <= 14 and ref is None:
            ref = tok
            continue

        break

    money_vals.sort(key=lambda x: x[0])

    saldo = money_vals[-1][1] if len(money_vals) >= 1 else None
    importe = money_vals[-2][1] if len(money_vals) >= 2 else None

    cut_idx_candidates = []
    if money_vals:
        cut_idx_candidates.append(money_vals[0][0])

    if ref is not None:
        for j in range(len(parts) - 1, -1, -1):
            if parts[j] == ref:
                cut_idx_candidates.append(j)
                break

    cut_idx = min(cut_idx_candidates) if cut_idx_candidates else len(parts)
    rest = " ".join(parts[:cut_idx]).strip()

    return importe, saldo, ref, rest


# =========================================================
# Modelo
# =========================================================
@dataclass
class Movimiento:
    fecha: Optional[datetime]
    concepto: str
    referencia: Optional[str]
    importe: Optional[float]  # leído
    saldo: Optional[float]    # leído
    # derivados
    delta_saldo: Optional[float] = None
    debito: float = 0.0
    credito: float = 0.0
    importe_inferido: Optional[float] = None
    ok_auditoria: Optional[bool] = None
    flags: str = ""


# =========================================================
# Parser principal
# =========================================================
def parse_movimientos_from_txt_text(text: str) -> List[Movimiento]:
    raw_lines = [_clean_ocr_weird_chars(x.rstrip("\n")) for x in text.splitlines()]
    movimientos: List[Movimiento] = []

    # movimiento “en armado”
    cur_fecha: Optional[datetime] = None
    cur_text_parts: List[str] = []
    cur_ref: Optional[str] = None
    cur_importe: Optional[float] = None
    cur_saldo: Optional[float] = None

    # movimientos pendientes de saldo (FIFO)
    pending: List[Movimiento] = []

    def flush_current_to_list_or_pending():
        """
        Cierra el movimiento actual:
        - Si tiene saldo => a 'movimientos'
        - Si NO tiene saldo pero tiene importe => a 'pending'
        """
        nonlocal cur_fecha, cur_text_parts, cur_ref, cur_importe, cur_saldo

        if cur_fecha is None:
            cur_text_parts = []
            cur_ref = None
            cur_importe = None
            cur_saldo = None
            return

        concepto = " ".join([p for p in cur_text_parts if p]).strip()

        if not (concepto or cur_importe is not None or cur_saldo is not None):
            cur_fecha = None
            cur_text_parts = []
            cur_ref = None
            cur_importe = None
            cur_saldo = None
            return

        m = Movimiento(
            fecha=cur_fecha,
            concepto=concepto,
            referencia=cur_ref,
            importe=cur_importe,
            saldo=cur_saldo,
        )

        if m.saldo is not None:
            movimientos.append(m)
        else:
            if m.importe is not None:
                pending.append(m)

        # reset
        cur_fecha = None
        cur_text_parts = []
        cur_ref = None
        cur_importe = None
        cur_saldo = None

    for line in raw_lines:
        line = line.strip()
        if not line or is_header_noise(line):
            continue

        if _looks_like_page_number_only(line):
            continue

        # 1) Línea SOLO SALDO: primero se la asignamos al movimiento actual si está abierto y le falta saldo.
        bal = _is_balance_only_line(line)
        if bal is not None:
            if cur_fecha is not None and cur_saldo is None and cur_importe is not None:
                cur_saldo = bal
                flush_current_to_list_or_pending()
            elif pending:
                pending[0].saldo = bal
                movimientos.append(pending.pop(0))
            else:
                # saldo suelto sin candidato (ruido)
                pass
            continue

        # 2) Si arranca con fecha => nuevo movimiento
        dt = parse_date(line)
        if dt is not None:
            flush_current_to_list_or_pending()
            cur_fecha = dt
            line = DATE_RE.sub("", line, count=1).strip()

        # 2.1) Si todavía no hay movimiento activo, ignoramos (texto fuera de la tabla)
        if cur_fecha is None:
            continue

        # 3) Ignorar líneas metadata típicas (no son movimiento)
        up = line.upper()
        if up.startswith("OPERACIÓN") or up.startswith("GENERADA EL"):
            continue
        if up.startswith("NRO COMERCIO") or up.startswith("MARCA:"):
            continue

        # 4) Extraer importe/saldo/ref de la cola numérica
        imp, sal, ref, rest = _extract_tail_money_and_ref(line)

        # ref desde "rest" si quedó ahí
        if cur_ref is None:
            mref = re.search(r"\b(\d{8,14})\b$", rest)
            if mref:
                cur_ref = mref.group(1)
                rest = rest[: mref.start(1)].strip()

        if ref and cur_ref is None:
            cur_ref = ref

        if rest:
            cur_text_parts.append(rest)

        if imp is not None:
            cur_importe = imp
        if sal is not None:
            cur_saldo = sal

        # NO flush acá: se cierra con nueva fecha o con saldo-only asignado o EOF

    flush_current_to_list_or_pending()

    # pendientes sin saldo al final => salen con saldo None para auditoría
    for m in pending:
        movimientos.append(m)

    return movimientos


# =========================================================
# Auditoría y clasificación Débito/Crédito
# =========================================================
def audit_and_classify(movs: List[Movimiento], tol: float = 0.01) -> List[Movimiento]:
    prev_saldo: Optional[float] = None

    for m in movs:
        flags: List[str] = []

        if m.saldo is None:
            flags.append("SIN_SALDO")
            m.ok_auditoria = False
            m.flags = ";".join(flags)
            continue

        if prev_saldo is None:
            # primer registro: no auditable por delta
            m.delta_saldo = None
            if m.importe is None:
                flags.append("PRIMERA_FILA_SIN_IMPORTE")
            m.ok_auditoria = True
            m.flags = ";".join(flags)
            prev_saldo = m.saldo
            continue

        delta = m.saldo - prev_saldo
        m.delta_saldo = delta

        # Clasificación por delta de saldo (regla principal)
        if abs(delta) <= tol:
            m.debito = 0.0
            m.credito = 0.0
            flags.append("DELTA_CERO")
        elif delta > 0:
            m.credito = round(delta, 2)
            m.debito = 0.0
        else:
            m.debito = round(-delta, 2)
            m.credito = 0.0

        # Importe: debe existir; si no, lo inferimos del delta
        if m.importe is None:
            m.importe_inferido = round(abs(delta), 2)
            flags.append("IMPORTE_INFERIDO_POR_SALDO")
        else:
            if abs(abs(delta) - abs(m.importe)) > max(tol, 0.01):
                flags.append("IMPORTE_NO_COINCIDE_CON_DELTA_SALDO")

        # Regla: no deben quedar movimientos sin importe
        if m.importe is None and m.importe_inferido is None:
            flags.append("SIN_IMPORTE")
            m.ok_auditoria = False
        else:
            m.ok_auditoria = ("IMPORTE_NO_COINCIDE_CON_DELTA_SALDO" not in flags)

        m.flags = ";".join(flags)
        prev_saldo = m.saldo

    return movs


# =========================================================
# DataFrames y Excel
# =========================================================
def movimientos_to_dfs(movs: List[Movimiento]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for idx, m in enumerate(movs, start=1):
        rows.append({
            "N": idx,
            "Fecha": m.fecha.strftime("%d/%m/%y") if m.fecha else None,
            "Concepto": m.concepto,
            "Referencia": str(m.referencia) if m.referencia is not None else None,
            "Importe_Leido": m.importe,
            "Importe_Inferido": m.importe_inferido,
            "Importe_Final": (m.importe if m.importe is not None else m.importe_inferido),
            "Débito": m.debito,
            "Crédito": m.credito,
            "Saldo": m.saldo,
            "Delta_Saldo": m.delta_saldo,
            "OK_Auditoría": m.ok_auditoria,
            "Flags": m.flags,
        })

    df_mov = pd.DataFrame(rows)

    df_aud = df_mov[(df_mov["OK_Auditoría"] == False) | (df_mov["Flags"].fillna("") != "")]
    df_aud = df_aud.sort_values(["OK_Auditoría", "N"], ascending=[True, True])

    return df_mov, df_aud


def build_excel_bytes(df_mov: pd.DataFrame, df_aud: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_mov.to_excel(writer, sheet_name="Movimientos", index=False)
        df_aud.to_excel(writer, sheet_name="Auditoría", index=False)

        for sheet_name in ["Movimientos", "Auditoría"]:
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                col_letter = col[0].column_letter
                max_len = 0
                for cell in col:
                    v = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(v))
                ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 60)

    output.seek(0)
    return output.read()


# =========================================================
# UI
# =========================================================
with st.sidebar:
    st.header("🧰 Cómo usar")
    st.markdown(
        """
1) Hacés OCR al PDF (vos ya lo hacés).  
2) Abrís el PDF y copiás el texto.  
3) Pegás en un `.txt`.  
4) Subís el `.txt` acá.  
5) Descargás el Excel.

**Regla clave:** Débito/Crédito se clasifica por **delta de saldo**.
        """
    )
    st.divider()
    tol = st.number_input("Tolerancia auditoría (pesos)", min_value=0.0, value=0.01, step=0.01, format="%.2f")


c1, c2 = st.columns([1.05, 1.25], gap="large")

with c1:
    st.subheader("📥 Cargar archivo")
    uploaded = st.file_uploader("Archivo .txt", type=["txt"])
    process = st.button("PROCESAR", type="primary", use_container_width=True, disabled=(uploaded is None))

with c2:
    st.subheader("📊 Vista previa")

    if "df_mov" not in st.session_state:
        st.session_state.df_mov = None
        st.session_state.df_aud = None
        st.session_state.excel_bytes = None

    if process and uploaded is not None:
        txt = uploaded.getvalue().decode("utf-8", errors="ignore")

        movs = parse_movimientos_from_txt_text(txt)
        movs = audit_and_classify(movs, tol=float(tol))

        df_mov, df_aud = movimientos_to_dfs(movs)
        excel_bytes = build_excel_bytes(df_mov, df_aud)

        st.session_state.df_mov = df_mov
        st.session_state.df_aud = df_aud
        st.session_state.excel_bytes = excel_bytes

    df_mov = st.session_state.df_mov
    df_aud = st.session_state.df_aud
    excel_bytes = st.session_state.excel_bytes

    if df_mov is None or df_mov.empty:
        st.info("Subí un TXT y tocá **PROCESAR** para ver la vista previa.")
    else:
        total = int(len(df_mov))
        errores = int(len(df_aud)) if df_aud is not None else 0

        saldo_inicial = df_mov["Saldo"].dropna().iloc[0] if df_mov["Saldo"].notna().any() else None
        saldo_final = df_mov["Saldo"].dropna().iloc[-1] if df_mov["Saldo"].notna().any() else None

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total movimientos", f"{total}")
        m2.metric("Saldo inicial", f"${saldo_inicial:,.2f}" if saldo_inicial is not None else "N/D")
        m3.metric("Saldo final", f"${saldo_final:,.2f}" if saldo_final is not None else "N/D")
        m4.metric("Errores", f"{errores}")

        st.dataframe(
            df_mov[["Fecha", "Concepto", "Importe_Final", "Débito", "Crédito", "Saldo", "Flags"]].head(60),
            use_container_width=True,
            hide_index=True
        )

        st.download_button(
            label="⬇️ Descargar Excel",
            data=excel_bytes,
            file_name="Extracto_ETL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

        with st.expander("Ver auditoría / flags (primeros 300)"):
            st.dataframe(df_aud.head(300), use_container_width=True, hide_index=True)
