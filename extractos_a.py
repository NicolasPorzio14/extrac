# app_streamlit_extracto.py
# Streamlit: subís el .txt (texto copiado del PDF con OCR ya hecho) y descarga un Excel:
# - Movimientos (fecha, concepto, referencia, importe leído/inferido, débito/crédito, saldo, delta)
# - Auditoría (flags / inconsistencias)
#
# Ejecutar:
#   pip install streamlit pandas openpyxl
#   streamlit run app_streamlit_extracto.py
#
# Nota:
#   Este parser es heurístico (porque depende de cómo quedó el OCR).
#   La lógica clave es: CLASIFICAR DÉBITO/CRÉDITO POR CAMBIO DE SALDO (delta saldo).

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st


# =========================================================
# Helpers numéricos / parsing
# =========================================================

def _clean_ocr_weird_chars(s: str) -> str:
    repl = {
        "Dв": "DB",
        "DВ": "DB",
        "ARBА": "ARBA",  # A cirílica
        "С": "C",        # C cirílica
        "О": "O",        # O cirílica
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    return s

def parse_ar_number(token: str) -> Optional[float]:
    """
    Parser robusto para OCR:
    - 1.234,56 / 1234,56
    - 1,234.56 / 1234.56
    - 1.648.32  (OCR: miles con puntos + decimal con punto)
    - -155.642.50 (OCR)
    """
    t = token.strip()
    if not t:
        return None
    if not re.fullmatch(r"[-+]?\d[\d\.,]*", t):
        return None

    sign = -1.0 if t.startswith("-") else 1.0
    t2 = t.lstrip("+-")

    # Caso OCR: múltiples puntos y el último grupo tiene 2 dígitos => último punto es decimal
    if "." in t2 and t2.count(".") >= 2 and "," not in t2:
        parts = t2.split(".")
        if len(parts[-1]) == 2 and all(p.isdigit() for p in parts):
            # miles = todo menos el último, decimal = último
            t2 = "".join(parts[:-1]) + "." + parts[-1]
        else:
            # si no cumple, asumimos que son miles
            t2 = t2.replace(".", "")

    # Caso OCR: múltiples comas y el último grupo tiene 2 dígitos => último es decimal
    if "," in t2 and t2.count(",") >= 2 and "." not in t2:
        parts = t2.split(",")
        if len(parts[-1]) == 2 and all(p.isdigit() for p in parts):
            t2 = "".join(parts[:-1]) + "." + parts[-1]
        else:
            t2 = t2.replace(",", "")

    # Mixtos coma+punto: decide por el último separador
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

DATE_RE = re.compile(r"^(?P<d>\d{2}/\d{2}/\d{2})\b")
NUMBER_TOKEN_RE = re.compile(r"[-+]?\d[\d\.,]*$")


def parse_date(s: str) -> Optional[datetime]:
    m = DATE_RE.match(s.strip())
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
        "I.V.A.",
        "FECHA CONCEPTO",
        "DÉBITO CRÉDITO SALDO",
        "DEBITO CREDITO SALDO",
        "SUBTOTAL",
        "C.U.I.T",
        "NRO COMERCIO",
        "MARCA:",
        "CBU:",
        "BENEF:",
        "OPERACIÓN",
        "GENERADA EL",
        "PÁGINA",
        "PAGINA",
    ]
    return any(x in l for x in noisy)


def _extract_tail_numbers_with_tokens(line: str) -> Tuple[List[Tuple[str, float]], str]:
    """
    Devuelve lista de pares (token_original, valor_float) extraídos desde el final.
    """
    parts = line.strip().split()
    out: List[Tuple[str, float]] = []

    while parts:
        tok = parts[-1]
        if not NUMBER_TOKEN_RE.fullmatch(tok):
            break
        val = parse_ar_number(tok)
        if val is None:
            break
        out.append((tok, val))
        parts.pop()

    out.reverse()
    rest = " ".join(parts).strip()
    return out, rest

# =========================================================
# Modelo
# =========================================================

@dataclass
class Movimiento:
    fecha: Optional[datetime]
    concepto: str
    referencia: Optional[str]
    importe: Optional[float]   # importe “leído” desde OCR (si existe)
    saldo: Optional[float]     # saldo “leído” desde OCR (si existe)
    # Derivados:
    delta_saldo: Optional[float] = None
    debito: float = 0.0
    credito: float = 0.0
    importe_inferido: Optional[float] = None
    ok_auditoria: Optional[bool] = None
    flags: str = ""


# =========================================================
# Parser movimientos
# =========================================================

def parse_movimientos_from_txt_text(text: str) -> List[Movimiento]:
    """
    Parser robusto:
    - Detecta inicio de movimiento por fecha DD/MM/YY.
    - Extrae números del final de la línea.
    - Interpreta cola numérica como:
        * ... IMPORTE SALDO
        * ... REF SALDO
        * ... REF IMPORTE SALDO
      donde REF es un entero largo (8–14 dígitos) sin separadores.
    - Si el OCR parte el movimiento en varias líneas, acumula texto y completa cuando aparecen números.
    """
    raw_lines = [_clean_ocr_weird_chars(x.rstrip("\n")) for x in text.splitlines()]

    movimientos: List[Movimiento] = []

    cur_fecha: Optional[datetime] = None
    cur_text_parts: List[str] = []
    cur_ref: Optional[str] = None
    cur_importe: Optional[float] = None
    cur_saldo: Optional[float] = None

    def flush_current():
        nonlocal cur_fecha, cur_text_parts, cur_ref, cur_importe, cur_saldo
        concepto = " ".join([p for p in cur_text_parts if p]).strip()
        if concepto or cur_importe is not None or cur_saldo is not None:
            movimientos.append(
                Movimiento(
                    fecha=cur_fecha,
                    concepto=concepto,
                    referencia=cur_ref,
                    importe=cur_importe,
                    saldo=cur_saldo,
                )
            )
        cur_fecha = None
        cur_text_parts = []
        cur_ref = None
        cur_importe = None
        cur_saldo = None

    def looks_like_ref_token(tok: str) -> bool:
        # referencia típica: 8 a 14 dígitos puros (sin comas/puntos), o sea "0970300052"
        raw = re.sub(r"[^\d]", "", tok)
        return raw.isdigit() and 8 <= len(raw) <= 14 and ("," not in tok) and ("." not in tok)

    def extract_tail_pairs(line: str) -> Tuple[List[Tuple[str, float]], str]:
        """
        Igual que _extract_tail_numbers, pero devuelve token crudo + float.
        """
        parts = line.strip().split()
        out: List[Tuple[str, float]] = []

        while parts:
            tok = parts[-1]
            if not NUMBER_TOKEN_RE.fullmatch(tok):
                break
            val = parse_ar_number(tok)
            if val is None:
                break
            out.append((tok, val))
            parts.pop()

        out.reverse()
        rest_ = " ".join(parts).strip()
        return out, rest_

    for line in raw_lines:
        line = line.strip()
        if not line or is_header_noise(line):
            continue

        # Nuevo movimiento si arranca con fecha
        dt = parse_date(line)
        if dt is not None:
            flush_current()
            cur_fecha = dt
            line = DATE_RE.sub("", line, count=1).strip()

        # Extrae pares (token, valor) al final
        pairs, rest = extract_tail_pairs(line)

        # 1) Referencia desde el texto (si termina en bloque largo)
        #    (Esto captura casos tipo: "Comision ... 0970300052 17,286.00 173,635.58"
        #     donde la ref queda en rest si no fue tomada como número por el parser)
        ref_match = re.search(r"\b(\d{8,14})\b$", rest)
        if ref_match and cur_ref is None:
            cur_ref = ref_match.group(1)
            rest = rest[: ref_match.start(1)].strip()

        if rest:
            cur_text_parts.append(rest)

        # 2) Interpretación de la cola numérica (desde derecha)
        #    último = saldo
        #    penúltimo = importe o referencia
        #    antepenúltimo = referencia (si existe)
        if len(pairs) >= 1:
            tok_saldo, val_saldo = pairs[-1]
            cur_saldo = val_saldo

        if len(pairs) == 2:
            tok2, val2 = pairs[-2]

            if looks_like_ref_token(tok2):
                # REF + SALDO (no hay importe en esta línea)
                cur_ref = re.sub(r"[^\d]", "", tok2)
                # no seteamos cur_importe acá
            else:
                # IMPORTE + SALDO
                cur_importe = val2

        elif len(pairs) >= 3:
            tok3, val3 = pairs[-3]
            tok2, val2 = pairs[-2]

            # patrón principal: REF + IMPORTE + SALDO
            if looks_like_ref_token(tok3):
                cur_ref = re.sub(r"[^\d]", "", tok3)
                cur_importe = val2
            # patrón alternativo: IMPORTE + REF + SALDO (OCR raro)
            elif looks_like_ref_token(tok2):
                cur_ref = re.sub(r"[^\d]", "", tok2)
                # en este caso no confiamos en importe (porque tok2 era ref)
                # pero si querés, podés intentar usar val3 como importe:
                cur_importe = val3
            else:
                # fallback: tomamos penúltimo como importe
                cur_importe = val2

        elif len(pairs) == 1:
            # Solo saldo o solo importe: priorizamos SALDO (tu requisito)
            # Ya lo setea el bloque len>=1 (cur_saldo = ...)
            pass

    flush_current()

    movimientos = [
        m for m in movimientos
        if (m.fecha is not None or m.concepto) and (m.saldo is not None or m.importe is not None)
    ]
    return movimientos



# =========================================================
# Auditoría y clasificación
# =========================================================

def audit_and_classify(movs: List[Movimiento], tol: float = 0.01) -> List[Movimiento]:
    prev_saldo: Optional[float] = None

    for m in movs:
        flags = []

        if m.saldo is None:
            flags.append("SIN_SALDO")
            m.ok_auditoria = False
            m.flags = ";".join(flags)
            continue

        if prev_saldo is None:
            m.delta_saldo = None
            if m.importe is None:
                flags.append("PRIMERA_FILA_SIN_IMPORTE")
            m.ok_auditoria = True
            m.flags = ";".join(flags)
            prev_saldo = m.saldo
            continue

        delta = m.saldo - prev_saldo
        m.delta_saldo = delta

        # Débito/Crédito por delta de saldo
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

        # Auditoría importe
        if m.importe is None:
            m.importe_inferido = round(abs(delta), 2)
            flags.append("IMPORTE_INFERIDO_POR_SALDO")
        else:
            if abs(abs(delta) - abs(m.importe)) > max(tol, 0.01):
                flags.append("IMPORTE_NO_COINCIDE_CON_DELTA_SALDO")

        if m.importe is None and m.importe_inferido is None:
            flags.append("SIN_IMPORTE")
            m.ok_auditoria = False
        else:
            m.ok_auditoria = ("IMPORTE_NO_COINCIDE_CON_DELTA_SALDO" not in flags)

        m.flags = ";".join(flags)
        prev_saldo = m.saldo

    return movs


def movimientos_to_dfs(movs: List[Movimiento]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for idx, m in enumerate(movs, start=1):
        rows.append({
            "N": idx,
            "fecha": m.fecha.strftime("%d/%m/%y") if m.fecha else None,
            "concepto": m.concepto,
            "referencia": m.referencia,
            "importe": m.importe if m.importe is not None else m.importe_inferido,
            "debito": m.debito,
            "credito": m.credito,
            "saldo": m.saldo,
            "delta_saldo": m.delta_saldo,
            "ok_auditoria": m.ok_auditoria,
            "flags": m.flags,
            "importe_leido": m.importe,
            "importe_inferido": m.importe_inferido,
        })

    df = pd.DataFrame(rows)

    # Errores: lo que no pasó auditoría o tiene flags
    df_errors = df[(df["ok_auditoria"] == False) | (df["flags"].fillna("") != "")]
    df_errors = df_errors.sort_values(["ok_auditoria", "N"], ascending=[True, True])

    return df, df_errors


def build_excel_bytes(df_mov: pd.DataFrame, df_aud: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_mov.to_excel(writer, sheet_name="Movimientos", index=False)
        df_aud.to_excel(writer, sheet_name="Auditoría", index=False)

        # Ajuste simple de anchos + freeze
        for sheet_name in ["Movimientos", "Auditoría"]:
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                col_letter = col[0].column_letter
                max_len = 0
                for cell in col:
                    v = "" if cell.value is None else str(cell.value)
                    if len(v) > max_len:
                        max_len = len(v)
                ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 60)

    output.seek(0)
    return output.read()


# =========================================================
# UI Streamlit (similar a tu captura)
# =========================================================

st.set_page_config(page_title="ETL Extracto → Excel", page_icon="📄", layout="wide")

st.title("📄 ETL Extracto bancario (TXT OCR) → Excel")
st.caption("Subí el TXT copiado del PDF (OCR ya hecho) y generá un Excel con auditoría lógica por SALDO.")

# Sidebar con parámetros
with st.sidebar:
    st.header("⚙️ Parámetros")
    tol = st.number_input("Tolerancia auditoría (pesos)", min_value=0.0, value=0.01, step=0.01, format="%.2f")
    st.markdown(
        """
**Regla clave:**
- Débito/Crédito se clasifica **por cambio de saldo** (delta).

**Auditoría:**
- Delta saldo debe coincidir con importe (leído o inferido).
- No deben quedar líneas sin importe.
        """
    )

col_left, col_right = st.columns([1.05, 1.25], gap="large")

# -------- Left: Upload + botón
with col_left:
    st.subheader("📥 Cargar Archivo")
    uploaded = st.file_uploader("Selecciona el archivo .txt", type=["txt"])

    process = st.button("PROCESAR", type="primary", use_container_width=True, disabled=(uploaded is None))

# -------- Right: Preview
with col_right:
    st.subheader("📊 Vista Previa")

    if "df_mov" not in st.session_state:
        st.session_state.df_mov = None
        st.session_state.df_aud = None
        st.session_state.excel_bytes = None

    if process and uploaded is not None:
        txt = uploaded.getvalue().decode("utf-8", errors="ignore")

        movs = parse_movimientos_from_txt_text(txt)
        movs = audit_and_classify(movs, tol=float(tol))

        df_mov, df_aud = movimientos_to_dfs(movs)

        st.session_state.df_mov = df_mov
        st.session_state.df_aud = df_aud
        st.session_state.excel_bytes = build_excel_bytes(df_mov, df_aud)

    df_mov = st.session_state.df_mov
    df_aud = st.session_state.df_aud
    excel_bytes = st.session_state.excel_bytes

    if df_mov is None or df_mov.empty:
        st.info("Subí un TXT y tocá **PROCESAR** para ver la vista previa.")
    else:
        # Métricas como tu captura
        saldo_inicial = df_mov["saldo"].dropna().iloc[0] if df_mov["saldo"].notna().any() else None
        saldo_final = df_mov["saldo"].dropna().iloc[-1] if df_mov["saldo"].notna().any() else None
        errores = int(len(df_aud)) if df_aud is not None else 0
        total = int(len(df_mov))

        m1, m2, m3 = st.columns(3)
        m1.metric("Total movimientos", f"{total}")
        m2.metric("Saldo inicial", f"${saldo_inicial:,.2f}" if saldo_inicial is not None else "N/D")
        m3.metric("Saldo final", f"${saldo_final:,.2f}" if saldo_final is not None else "N/D")

        m4, _ = st.columns([1, 2])
        m4.metric("Errores", f"{errores}")

        # Tabla (preview)
        preview_cols = ["fecha", "concepto", "importe", "debito", "credito", "saldo"]
        st.dataframe(
            df_mov[preview_cols].head(30),
            use_container_width=True,
            hide_index=True
        )

        # Botón descarga Excel
        st.download_button(
            label="⬇️ Descargar Excel",
            data=excel_bytes,
            file_name="Extracto_ETL.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        # Panel de auditoría opcional
        with st.expander("Ver auditoría / flags (primeros 200)"):
            st.dataframe(
                df_aud.head(200),
                use_container_width=True,
                hide_index=True
            )

