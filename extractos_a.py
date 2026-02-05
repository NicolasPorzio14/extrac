# app.py
# Streamlit ETL: TXT (copiado de PDF con OCR) -> Excel (Movimientos + Auditoría)
#
# Ejecutar:
#   pip install streamlit pandas openpyxl
#   streamlit run app.py
#
# Qué hace:
# - Subís un .txt con el contenido copiado del PDF (OCR ya hecho).
# - Parseo robusto: fecha, concepto, referencia, importe, saldo.
# - Maneja casos NO lineales típicos (conceptos en una línea + montos/saldos en líneas siguientes).
# - Débito/Crédito se clasifica por delta de saldo (regla principal).
# - Auditoría: compara delta vs importe y marca inconsistencias.
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
    r"^[+-]?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})$|^[+-]?\d+(?:[.,]\d{2})$"
)
INT_TOKEN_RE = re.compile(r"^\d+$")

NOISY_SUBSTR = [
    "DETALLE DE MOVIMIENTOS",
    "RESUMEN DE CUENTA",
    "CUENTA CORRIENTE",
    "FECHA CONCEPTO",
    "DÉBITO CRÉDITO SALDO",
    "DEBITO CREDITO SALDO",
    "I.V.A. RESPONSABLE",
    "C.U.I.T",
    "NRO COMERCIO",
    "MARCA:",
    "CBU:",
    "BENEF:",
    "PÁGINA",
    "PAGINA",
]

KEYWORDS_NEW = (
    "IMPUESTO",
    "COMERCIOS",
    "CREDITO",
    "CRÉDITO",
    "DEBITO",
    "DÉBITO",
    "PAGO",
    "COM.",
    "COMISION",
    "IVA",
    "PERCEPC",
    "COBRO",
    "RETEN",
    "TRANSFER",
    "DEBIN",
    "CHEQUE",
    "DEPÓSITO",
    "DEPOSITO",
    "DÉBITOS",
)


def _clean_ocr_weird_chars(s: str) -> str:
    """Normaliza caracteres raros típicos del OCR."""
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
    t = token.strip().replace("$", "")
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
        if t2.rfind(",") > t2.rfind("."):
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
    u = line.strip().upper()
    if not u:
        return True
    if u.startswith("SUBTOTAL"):
        return True
    return any(x in u for x in NOISY_SUBSTR)


def _looks_like_page_number_only(line: str) -> bool:
    return bool(re.fullmatch(r"\d{1,4}", line.strip()))


def _is_metadata_line(line: str) -> bool:
    u = line.strip().upper()
    return (
        u.startswith("OPERACIÓN")
        or u.startswith("OPERACION")
        or u.startswith("GENERADA EL")
        or u.startswith("NRO COMERCIO")
        or u.startswith("MARCA:")
        or u.startswith("AFIP ID:")
        or u.startswith("PRES:")
        or u.startswith("IDENTIFICACION:")
        or u.startswith("REF:")
        or u.startswith("PYME ")
    )


def _probable_new_concept_line(line: str) -> bool:
    u = line.strip().upper()
    return any(u.startswith(k) for k in KEYWORDS_NEW)


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


def _extract_tail_money_and_ref(line: str) -> Tuple[Optional[float], Optional[float], Optional[str], str, int]:
    """
    Extrae desde el final:
      - saldo = último token monetario
      - importe = token monetario anterior al saldo
      - referencia = entero largo (8-14 dígitos) cercano al final
    Devuelve: (importe, saldo, referencia, texto_restante, cantidad_montos_detectados)
    """
    parts = line.strip().split()
    if not parts:
        return None, None, None, "", 0

    money_vals: List[Tuple[int, float]] = []
    ref: Optional[str] = None
    tail_start = len(parts)

    for i in range(len(parts) - 1, -1, -1):
        tok = parts[i]

        if MONEY_TOKEN_RE.fullmatch(tok):
            val = parse_ar_number(tok)
            if val is not None:
                money_vals.append((i, val))
                tail_start = min(tail_start, i)
            continue

        if INT_TOKEN_RE.fullmatch(tok) and 8 <= len(tok) <= 14 and ref is None:
            ref = tok
            tail_start = min(tail_start, i)
            continue

        break  # salimos de la "cola" numérica

    money_vals.sort(key=lambda x: x[0])

    saldo = money_vals[-1][1] if len(money_vals) >= 1 else None
    importe = money_vals[-2][1] if len(money_vals) >= 2 else None
    rest = " ".join(parts[:tail_start]).strip()

    return importe, saldo, ref, rest, len(money_vals)


# =========================================================
# Modelo
# =========================================================
@dataclass
class Movimiento:
    fecha: Optional[datetime]
    concepto: str
    referencia: Optional[str]
    importe: Optional[float]  # leído / reconstruido
    saldo: Optional[float]    # leído / reconstruido
    # derivados
    delta_saldo: Optional[float] = None
    debito: float = 0.0
    credito: float = 0.0
    importe_inferido: Optional[float] = None
    ok_auditoria: Optional[bool] = None
    flags: str = ""


# =========================================================
# Parser principal (state machine)
# =========================================================
def parse_movimientos_from_txt_text(text: str) -> List[Movimiento]:
    """
    Parser robusto:
    - Soporta movimientos “partidos”: concepto en una línea y (ref/importe/saldo) en líneas siguientes.
    - Soporta múltiples conceptos seguidos y luego múltiples líneas de importes/saldos.
    - Evita crear “movimientos fantasma” con líneas descriptivas (ej. CAPTAIN HOPS SAS).
    """
    raw_lines = [_clean_ocr_weird_chars(x.rstrip("\n")) for x in text.splitlines()]

    movimientos: List[Movimiento] = []
    pending: List[Movimiento] = []  # movimientos a completar (esperan importe/saldo)
    last_date: Optional[datetime] = None
    last_completed: Optional[Movimiento] = None

    # para ignorar un saldo “arrastre” luego de SUBTOTAL / headers o de la palabra "Saldo"
    ignore_next_balance_only = False

    for raw in raw_lines:
        line = raw.strip()
        if not line:
            continue

        upper = line.upper()

        # Palabra suelta "Saldo" (en algunos PDFs queda en una línea sola)
        if upper == "SALDO":
            ignore_next_balance_only = True
            continue

        # Ruido/header/paginado
        if _looks_like_page_number_only(line) or is_header_noise(line):
            if upper.startswith("SUBTOTAL") or ("DÉBITO" in upper and "SALDO" in upper):
                ignore_next_balance_only = True
            continue

        # 1) Si es saldo-only line
        bal = _is_balance_only_line(line)
        if bal is not None:
            if ignore_next_balance_only and not pending:
                ignore_next_balance_only = False
                continue

            # si hay pending, a veces el primer saldo es el arrastre de cabecera: ignorar 1 (y luego asignar)
            if ignore_next_balance_only and pending:
                ignore_next_balance_only = False
                continue

            ignore_next_balance_only = False

            # asignar al primer pendiente sin saldo
            target = next((m for m in pending if m.saldo is None), None)
            if target is not None:
                target.saldo = bal
                movimientos.append(target)
                last_completed = target
                pending.remove(target)
            continue

        ignore_next_balance_only = False

        # 2) Línea con fecha => nuevo movimiento (aunque el importe/saldo estén en líneas siguientes)
        dt = parse_date(line)
        if dt is not None:
            last_date = dt
            rest = DATE_RE.sub("", line, count=1).strip()

            if not rest:
                # "dd/mm/yy" solo => artefacto
                continue

            if _is_metadata_line(rest):
                continue

            imp, sal, ref, texto, mcount = _extract_tail_money_and_ref(rest)

            m = Movimiento(
                fecha=dt,
                concepto=texto.strip(),
                referencia=ref,
                importe=None,
                saldo=None,
            )

            if mcount >= 2:
                m.importe = imp
                m.saldo = sal
                movimientos.append(m)
                last_completed = m
            elif mcount == 1:
                # solo un monto en la misma línea => normalmente es IMPORTE
                m.importe = sal
                pending.append(m)
            else:
                pending.append(m)

            continue

        # 3) Línea sin fecha
        if _is_metadata_line(line):
            continue

        imp, sal, ref, texto, mcount = _extract_tail_money_and_ref(line)

        # 3A) Línea con importe+saldo (2 montos): completa un pendiente
        if mcount >= 2:
            target = next((m for m in pending if m.saldo is None), None)

            # si no hay pending, creamos un movimiento sintético (última fecha conocida)
            if target is None:
                if last_date is None:
                    continue
                target = Movimiento(last_date, texto.strip(), None, None, None)

            if target.referencia is None and ref:
                target.referencia = ref
            if not target.concepto and texto:
                target.concepto = texto.strip()

            target.importe = imp
            target.saldo = sal

            movimientos.append(target)
            last_completed = target
            if target in pending:
                pending.remove(target)
            continue

        # 3B) Línea con un solo monto (típico: concepto + importe, saldo vendrá en línea siguiente)
        if mcount == 1:
            one_val = sal  # en nuestro extractor, 'sal' contiene el único monto detectado
            if one_val is None:
                continue

            target = next((m for m in pending if m.importe is None), None)
            if target is None and pending:
                target = pending[-1]

            if target is not None:
                target.importe = one_val
                if target.referencia is None and ref:
                    target.referencia = ref
                if not target.concepto and texto:
                    target.concepto = texto.strip()
            continue

        # 3C) Texto puro (sin montos): puede ser:
        # - continuación del concepto de un pendiente (ej. "CAPTAIN HOPS SAS")
        # - un nuevo concepto de movimiento (pendiente) en la misma fecha
        if last_date is None:
            continue

        if pending and pending[-1].saldo is None:
            if _probable_new_concept_line(line):
                pending.append(Movimiento(last_date, line.strip(), None, None, None))
            else:
                pending[-1].concepto = (pending[-1].concepto + " " + line).strip()
        else:
            # sin pending: si el último movimiento fue "Comercios First Data", esto suele ser detalle del comercio
            if last_completed is not None and last_completed.fecha == last_date:
                if "COMERCIOS FIRST DATA" in last_completed.concepto.upper() and not _probable_new_concept_line(line):
                    last_completed.concepto = (last_completed.concepto + " " + line).strip()
                    continue

            # si parece un concepto real, lo creamos; si no, lo ignoramos (evita movimientos fantasma)
            if _probable_new_concept_line(line):
                pending.append(Movimiento(last_date, line.strip(), None, None, None))

    # lo que quede pendiente, se exporta igual (para auditoría)
    movimientos.extend(pending)
    return movimientos


# =========================================================
# Reglas opcionales: reparación de desfasaje de importes
# =========================================================
def repair_shift_importes(movs: List[Movimiento], tol: float = 0.01) -> List[Movimiento]:
    """
    Repara desfasajes típicos OCR/TXT donde el importe quedó en la fila de arriba/abajo.
    Regla:
      - expected_i = abs(saldo_i - saldo_{i-1})
      - si importe_i no coincide con expected_i pero coincide con expected_{i+1} => mover abajo
      - si importe_i no coincide con expected_i pero coincide con expected_{i-1} => mover arriba
    """
    def close(a: Optional[float], b: Optional[float]) -> bool:
        if a is None or b is None:
            return False
        return abs(abs(a) - abs(b)) <= max(tol, 0.01)

    expected: List[Optional[float]] = [None] * len(movs)
    prev_saldo: Optional[float] = None

    for i, m in enumerate(movs):
        if m.saldo is None:
            expected[i] = None
            continue
        if prev_saldo is None:
            expected[i] = None
        else:
            expected[i] = abs(m.saldo - prev_saldo)
        prev_saldo = m.saldo

    # Shift hacia abajo
    for i in range(1, len(movs) - 1):
        mi = movs[i]
        mj = movs[i + 1]
        if mi.importe is None or expected[i] is None or expected[i + 1] is None:
            continue
        if (not close(mi.importe, expected[i])) and close(mi.importe, expected[i + 1]):
            if (mj.importe is None) or (expected[i + 1] is not None and not close(mj.importe, expected[i + 1])):
                mj.importe = mi.importe
                mi.importe = None

    # Shift hacia arriba
    for i in range(2, len(movs)):
        mi = movs[i]
        mp = movs[i - 1]
        if mi.importe is None or expected[i] is None or expected[i - 1] is None:
            continue
        if (not close(mi.importe, expected[i])) and close(mi.importe, expected[i - 1]):
            if (mp.importe is None) or (expected[i - 1] is not None and not close(mp.importe, expected[i - 1])):
                mp.importe = mi.importe
                mi.importe = None

    return movs


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
            m.delta_saldo = None
            if m.importe is None:
                flags.append("PRIMERA_FILA_SIN_IMPORTE")
            m.ok_auditoria = True
            m.flags = ";".join(flags)
            prev_saldo = m.saldo
            continue

        delta = m.saldo - prev_saldo
        m.delta_saldo = delta

        # Clasificación por delta de saldo
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

        # Auditoría: delta vs importe
        if m.importe is None:
            m.importe_inferido = round(abs(delta), 2)
            flags.append("IMPORTE_INFERIDO_POR_SALDO")
        else:
            if abs(abs(delta) - abs(m.importe)) > max(tol, 0.01):
                flags.append("IMPORTE_NO_COINCIDE_CON_DELTA_SALDO")

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

        # Auto-anchos + freeze
        for sheet_name in ["Movimientos", "Auditoría"]:
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"
            for col in ws.columns:
                col_letter = col[0].column_letter
                max_len = 0
                for cell in col:
                    v = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(v))
                ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 70)

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
    apply_shift_fix = st.checkbox("Aplicar reparación de desfasaje de importes (arriba/abajo)", value=False)


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

        if apply_shift_fix:
            movs = repair_shift_importes(movs, tol=float(tol))

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
        m4.metric("Observaciones", f"{errores}")

        st.dataframe(
            df_mov[["Fecha", "Concepto", "Importe_Final", "Débito", "Crédito", "Saldo", "Flags"]].head(80),
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

        with st.expander("Ver auditoría / flags (primeros 400)"):
            st.dataframe(df_aud.head(400), use_container_width=True, hide_index=True)
