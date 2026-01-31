import streamlit as st
import tempfile
from pathlib import Path
import pandas as pd
import re

# =========================
# CONFIG UI
# =========================
st.set_page_config(
    page_title="ETLs de Extractos → Excel",
    page_icon="📄",
    layout="wide"
)

st.title("📄 ETLs de Extractos Bancarios → Excel")
st.caption("Subí el TXT copiado desde un PDF con OCR (hecho fuera de esta app) y descargá el Excel listo para analizar.")

# Sidebar (global)
with st.sidebar:
    st.header("🧰 Cómo usar")
    st.markdown(
        """
1) Generá un PDF copiable con OCR (ej. Colab)  
2) Abrí el PDF y copia el contenido  
3) Pegalo en un `.txt`  
4) Subí el `.txt` en el ETL correcto (tab correspondiente)  
5) Descargá el Excel
        """
    )
    st.divider()
    st.markdown("**Sugerencia:** guardá el `.txt` en UTF-8 si podés (igual manejamos errores).")


# =========================
# ETL #1: BANCO ACTUAL (TXT -> XLSX)
# =========================
def etl_banco_actual_txt_to_excel_bytes(txt_bytes: bytes):
    # ---------- Regex base ----------
    date_re = re.compile(r"^\d{2}/\d{2}/\d{2}\b")
    pure_digits_re = re.compile(r"^\d+$")

    HEADER_MARKERS = {
        "DETALLE DE MOVIMIENTOS",
        "Fecha Concepto",
        "Fecha Concepto Débito Crédito Saldo",
        "Débito Crédito Saldo",
        "Debito Credito Saldo",
    }

    def is_amount_token(tok: str) -> bool:
        # referencias = solo dígitos => NO importe
        if pure_digits_re.match(tok):
            return False
        return bool(re.fullmatch(r"-?[\d\.,]+", tok)) and ("," in tok or "." in tok or tok.startswith("-"))

    def is_header(line: str) -> bool:
        up = line.upper()
        if line in HEADER_MARKERS:
            return True
        if up.startswith(("I.V.A.", "RESUMEN DE CUENTA", "CUENTA CORRIENTE", "C.U.I.T.")) or "C.U.I.T." in up:
            return True
        return False

    def normalize_amount(s: str):
        s = s.strip().replace(" ", "")
        s = (s.replace("DВ", "DB").replace("Dв", "DB")
               .replace("АРБА", "ARBA").replace("АРBА", "ARBA")
               .replace("CАРTAIN", "CAPTAIN"))
        s = re.sub(r"[^0-9\-,\.]", "", s)
        if s in {"", "-", ".", ",", "-.", "-,"}:
            return None

        neg = s.startswith("-")
        s2 = s[1:] if neg else s

        if "." in s2 and "," in s2:
            last_dot = s2.rfind(".")
            last_com = s2.rfind(",")
            dec = "." if last_dot > last_com else ","
            thou = "," if dec == "." else "."
            s2 = s2.replace(thou, "")
            if dec == ",":
                s2 = s2.replace(",", ".")
        else:
            if "," in s2:
                parts = s2.split(",")
                if len(parts[-1]) == 2:
                    s2 = "".join(parts[:-1]) + "." + parts[-1]
                else:
                    s2 = s2.replace(",", "")
            elif "." in s2:
                parts = s2.split(".")
                if len(parts[-1]) == 2:
                    s2 = "".join(parts[:-1]) + "." + parts[-1]
                else:
                    if len(parts) >= 3 and len(parts[-1]) == 2:
                        s2 = "".join(parts[:-1]) + "." + parts[-1]
                    else:
                        s2 = s2.replace(".", "")

        try:
            val = float(s2)
            return -val if neg else val
        except Exception:
            return None

    def extract_tail_amounts(tokens):
        rest = tokens[:]
        amts = []
        while rest and is_amount_token(rest[-1]):
            amts.append(rest.pop())
        amts.reverse()
        return rest, amts

    def fallback_side(concept: str) -> str:
        c = (concept or "").lower()
        if "/cr" in c or "credito" in c or "comercios first data" in c:
            return "credito"
        if "/db" in c or "debito" in c:
            return "debito"
        if any(k in c for k in ["comision", "impuesto", "retención", "retencion", "percepción", "percepcion", "iva", "pago"]):
            return "debito"
        return "credito"

    text = txt_bytes.decode("utf-8", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    rows = []
    pending_detail = []
    pending_saldo_indices = []   # FIFO de movimientos sin saldo todavía
    opening_saldo = None
    last_movement_idx = None

    def attach_detail(idx):
        nonlocal pending_detail
        if idx is None or not pending_detail:
            return
        prev = rows[idx].get("Detalle") or ""
        add = "\n".join(pending_detail)
        rows[idx]["Detalle"] = (prev + ("\n" if prev else "") + add).strip()
        pending_detail.clear()

    for ln in lines:
        if is_header(ln):
            continue

        up = ln.upper()

        # números de página
        if re.fullmatch(r"\d{1,4}", ln):
            continue

        # SUBTOTAL (primero = saldo inicial)
        if up.startswith("SUBTOTAL"):
            nums = re.findall(r"-?[\d\.,]+", ln)
            st = normalize_amount(nums[-1]) if nums else None
            rows.append({
                "Tipo": "SUBTOTAL",
                "Fecha": None,
                "Concepto": "SUBTOTAL",
                "Referencia": None,
                "Importe_Parsed": None,
                "Debito": None,
                "Credito": None,
                "Saldo": st,
                "Detalle": None
            })
            last_movement_idx = None
            if opening_saldo is None and st is not None:
                opening_saldo = st
            continue

        # detalles (debug)
        if up.startswith((
            "OPERACIÓN", "OPERACION",
            "NRO COMERCIO", "NRO COMERCIO:",
            "CAPTAIN", "PYME",
            "IDENTIFICACION:", "IDENTIFICACIÓN:",
            "329845"
        )):
            pending_detail.append(ln)
            continue

        # saldo suelto
        if re.fullmatch(r"-?[\d\.,]+", ln) and not date_re.match(ln) and ("," in ln or "." in ln or ln.startswith("-")):
            val = normalize_amount(ln)
            if val is not None and pending_saldo_indices:
                idx = pending_saldo_indices.pop(0)
                attach_detail(idx)
                rows[idx]["Saldo"] = val
            else:
                pending_detail.append(ln)
            continue

        # movimiento con fecha
        if date_re.match(ln):
            attach_detail(last_movement_idx)

            tokens = ln.split()
            fecha = tokens[0]
            rest, amt_tokens = extract_tail_amounts(tokens[1:])

            saldo = None
            mov_amt = None

            if len(amt_tokens) >= 2:
                mov_amt = normalize_amount(amt_tokens[-2])
                saldo = normalize_amount(amt_tokens[-1])
            elif len(amt_tokens) == 1:
                mov_amt = normalize_amount(amt_tokens[0])

            referencia = None
            for t in reversed(rest):
                if re.fullmatch(r"\d{6,}", t):
                    referencia = t
                    break

            concepto_tokens = rest[:]
            if referencia and referencia in concepto_tokens:
                for i in range(len(concepto_tokens) - 1, -1, -1):
                    if concepto_tokens[i] == referencia:
                        concepto_tokens.pop(i)
                        break

            concepto = " ".join(concepto_tokens).strip()

            rows.append({
                "Tipo": "MOVIMIENTO",
                "Fecha": fecha,
                "Concepto": concepto,
                "Referencia": referencia,
                "Importe_Parsed": mov_amt,
                "Debito": None,
                "Credito": None,
                "Saldo": saldo,
                "Detalle": None
            })

            last_movement_idx = len(rows) - 1
            if saldo is None:
                pending_saldo_indices.append(last_movement_idx)
            continue

        pending_detail.append(ln)

    attach_detail(last_movement_idx)

    raw_df = pd.DataFrame(rows)
    mov = raw_df[raw_df["Tipo"] == "MOVIMIENTO"].copy().reset_index(drop=True)

    for c in ["Importe_Parsed", "Saldo"]:
        mov[c] = pd.to_numeric(mov[c], errors="coerce")

    # Debito/Credito por delta de saldo
    tol = 0.05
    prev_saldo = opening_saldo

    for i in range(len(mov)):
        saldo = mov.at[i, "Saldo"]
        imp = mov.at[i, "Importe_Parsed"]

        if pd.isna(saldo) or prev_saldo is None or pd.isna(prev_saldo):
            mov.at[i, "Debito"] = float("nan")
            mov.at[i, "Credito"] = float("nan")
            if not pd.isna(imp):
                if fallback_side(str(mov.at[i, "Concepto"])) == "credito":
                    mov.at[i, "Credito"] = abs(float(imp))
                else:
                    mov.at[i, "Debito"] = abs(float(imp))
            prev_saldo = saldo if not pd.isna(saldo) else prev_saldo
            continue

        delta = saldo - prev_saldo
        if abs(delta) <= tol:
            mov.at[i, "Debito"] = float("nan")
            mov.at[i, "Credito"] = float("nan")
        else:
            amt = abs(delta)
            if delta > 0:
                mov.at[i, "Credito"] = amt
                mov.at[i, "Debito"] = float("nan")
            else:
                mov.at[i, "Debito"] = amt
                mov.at[i, "Credito"] = float("nan")

        prev_saldo = saldo

    mov_final = mov.drop(columns=["Tipo", "Importe_Parsed", "Referencia", "Detalle"], errors="ignore")

    # Excel bytes
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        mov_final.to_excel(writer, sheet_name="Movimientos", index=False)
        raw_df.to_excel(writer, sheet_name="Raw_Parse", index=False)

    excel_bytes = tmp_path.read_bytes()
    tmp_path.unlink(missing_ok=True)

    return excel_bytes, mov_final, raw_df


# =========================
# “Framework” para futuros ETLs
# =========================
def tab_banco_actual():
    st.subheader("🏦 Banco X (TXT → Excel)")
    st.write("Subí el `.txt` con el texto pegado desde el PDF con OCR y descargá el Excel.")

    st.info(
        "Tip: Pegá todo el bloque **Detalle de movimientos**. "
        "Si el saldo aparece en líneas separadas, el parser lo reasigna en orden."
    )

    txt_file = st.file_uploader("📎 Cargar TXT", type=["txt"], key="txt_banco_actual")

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        if txt_file is not None:
            if st.button("🧩 Generar Excel", type="primary", key="btn_banco_actual"):
                with st.spinner("Procesando TXT y armando Excel..."):
                    try:
                        excel_bytes, mov_final, raw_df = etl_banco_actual_txt_to_excel_bytes(txt_file.read())
                        st.session_state["preview_df"] = mov_final

                        st.success("✅ Listo.")
                        st.download_button(
                            "⬇️ Descargar Excel",
                            data=excel_bytes,
                            file_name="Extracto_ETL.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                        with st.expander("Ver debug (Raw_Parse)"):
                            st.dataframe(raw_df, use_container_width=True)

                    except Exception as e:
                        st.exception(e)
        else:
            st.warning("Esperando un archivo TXT...")

    with col2:
        st.markdown("### 👀 Vista previa")
        preview_df = st.session_state.get("preview_df", None)
        if preview_df is None:
            st.caption("Cuando generes el Excel, acá vas a ver las primeras filas.")
        else:
            st.dataframe(preview_df.head(30), use_container_width=True)

def tab_placeholder(nombre: str):
    st.subheader(f"🧩 {nombre} (próximamente)")
    st.write(
        "Este tab está preparado para sumar un ETL nuevo en el futuro. "
        "La idea es que cada banco/estructura tenga su propio parser."
    )
    st.info("Cuando quieras sumar otro, se agrega una función `etl_xxx()` y se conecta acá.")


# =========================
# Tabs
# =========================
tabs = st.tabs([
    "🏦 Banco X (actual)",
    "➕ Otro ETL (placeholder)",
    "➕ Otro ETL (placeholder 2)"
])

with tabs[0]:
    tab_banco_actual()

with tabs[1]:
    tab_placeholder("Banco Y")

with tabs[2]:
    tab_placeholder("Banco Z")

st.divider()
st.caption("App preparada para múltiples ETLs: cada tab puede tener su propio parser y validaciones.")

