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
    import re
    import tempfile
    from pathlib import Path
    import pandas as pd
    import numpy as np

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
        if pure_digits_re.match(tok):
            return False
        tok = tok.strip()
        return bool(re.fullmatch(r"-?[\d\.,]+", tok)) and any(ch in tok for ch in [",", "."])

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

    text = txt_bytes.decode("utf-8", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    rows = []
    pending_detail = []
    pending_indices = []  # movimientos que aún requieren (importe y/o saldo)
    opening_saldo = None
    last_movement_idx = None

    last_subtotal_saldo = None
    page_boundary_active = False  # se activa con SUBTOTAL real o "DETALLE DE MOVIMIENTOS"

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
            if ln == "DETALLE DE MOVIMIENTOS":
                page_boundary_active = True
            continue

        up = ln.upper()

        # números de página (pero ojo: si fuera "13 93,416.95" no entra acá por el espacio)
        if re.fullmatch(r"\d{1,4}", ln):
            continue

        # SUBTOTAL: solo si realmente tiene número (si dice solo "SUBTOTAL", es un artefacto del encabezado)
        if up.startswith("SUBTOTAL"):
            if re.search(r"\d", ln):
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
                last_subtotal_saldo = st
                page_boundary_active = True
            continue

        # líneas “detalle” típicas
        if up.startswith((
            "OPERACIÓN", "OPERACION",
            "NRO COMERCIO", "NRO COMERCIO:",
            "CAPTAIN", "PYME",
            "IDENTIFICACION:", "IDENTIFICACIÓN:"
        )):
            pending_detail.append(ln)
            continue

        # ===== 1) LÍNEAS SIN FECHA CON IMPORTES AL FINAL (page breaks) =====
        if pending_indices and not date_re.match(ln):
            toks = ln.split()
            # tomo la “cola” de tokens numéricos (al final)
            tail = []
            j = len(toks) - 1
            while j >= 0 and is_amount_token(toks[j]):
                tail.append(toks[j])
                j -= 1
            tail = list(reversed(tail))

            if len(tail) >= 2:
                saldo_val = normalize_amount(tail[-1])

                # Caso A: [importe, saldo]
                if len(tail) == 2:
                    amt_val = normalize_amount(tail[0])
                    if amt_val is not None and saldo_val is not None:
                        idx = pending_indices.pop(0)
                        attach_detail(idx)
                        if rows[idx].get("Importe_Parsed") is None:
                            rows[idx]["Importe_Parsed"] = amt_val
                        rows[idx]["Saldo"] = saldo_val
                        page_boundary_active = False
                        continue

                # Caso B: [debito, credito, saldo]
                if len(tail) == 3:
                    d = normalize_amount(tail[0])
                    c = normalize_amount(tail[1])
                    if saldo_val is not None and (d is not None or c is not None):
                        idx = pending_indices.pop(0)
                        attach_detail(idx)
                        rows[idx]["Saldo"] = saldo_val
                        # si viene bien definido, guardo débito/crédito directo
                        if d not in (None, 0.0) and (c in (None, 0.0)):
                            rows[idx]["Debito"] = abs(d)
                            rows[idx]["Credito"] = None
                            if rows[idx].get("Importe_Parsed") is None:
                                rows[idx]["Importe_Parsed"] = abs(d)
                        elif c not in (None, 0.0) and (d in (None, 0.0)):
                            rows[idx]["Credito"] = abs(c)
                            rows[idx]["Debito"] = None
                            if rows[idx].get("Importe_Parsed") is None:
                                rows[idx]["Importe_Parsed"] = abs(c)
                        page_boundary_active = False
                        continue

        # ===== 2) SALDO SUELTO (1 número) =====
        if re.fullmatch(r"-?[\d\.,]+", ln) and not date_re.match(ln) and any(ch in ln for ch in [",", "."]):
            val = normalize_amount(ln)
            if val is not None and pending_indices:
                idx0 = pending_indices[0]

                # si es saldo repetido del último subtotal (page break), lo ignoro
                if page_boundary_active and last_subtotal_saldo is not None and abs(val - last_subtotal_saldo) < 0.0001 and rows[idx0].get("Importe_Parsed") is None:
                    continue

                idx = pending_indices.pop(0)
                attach_detail(idx)
                rows[idx]["Saldo"] = val
                if not (last_subtotal_saldo is not None and abs(val - last_subtotal_saldo) < 0.0001):
                    page_boundary_active = False
            else:
                pending_detail.append(ln)
            continue

        # ===== 3) MOVIMIENTO CON FECHA =====
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
            if saldo is None or mov_amt is None:
                pending_indices.append(last_movement_idx)
            continue

        pending_detail.append(ln)

    attach_detail(last_movement_idx)

    raw_df = pd.DataFrame(rows)
    mov = raw_df[raw_df["Tipo"] == "MOVIMIENTO"].copy().reset_index(drop=True)

    for c in ["Importe_Parsed", "Saldo", "Debito", "Credito"]:
        if c in mov.columns:
            mov[c] = pd.to_numeric(mov[c], errors="coerce")

    # Débito/Credito por delta de saldo (SIN perder importes chicos)
    tol = 0.0001
    prev_saldo = opening_saldo

    for i in range(len(mov)):
        # si ya quedó debit/credit explícito por parse (caso 3 importes), lo dejo
        if (not pd.isna(mov.at[i, "Debito"])) or (not pd.isna(mov.at[i, "Credito"])):
            if not pd.isna(mov.at[i, "Saldo"]):
                prev_saldo = mov.at[i, "Saldo"]
            continue

        saldo = mov.at[i, "Saldo"]
        imp = mov.at[i, "Importe_Parsed"]

        mov.at[i, "Debito"] = np.nan
        mov.at[i, "Credito"] = np.nan

        if pd.isna(saldo) or prev_saldo is None or pd.isna(prev_saldo):
            if not pd.isna(imp):
                # fallback mínimo si no hay saldo
                c = str(mov.at[i, "Concepto"]).lower()
                if "comercios first data" in c or "crédito" in c:
                    mov.at[i, "Credito"] = abs(float(imp))
                else:
                    mov.at[i, "Debito"] = abs(float(imp))
            prev_saldo = saldo if not pd.isna(saldo) else prev_saldo
            continue

        delta = saldo - prev_saldo

        if abs(delta) <= tol:
            # delta casi 0, pero si hay importe, lo uso (clave para 0.02, 0.04, 0.06, etc.)
            if not pd.isna(imp) and abs(float(imp)) > tol:
                c = str(mov.at[i, "Concepto"]).lower()
                if "comercios first data" in c or "crédito" in c:
                    mov.at[i, "Credito"] = abs(float(imp))
                else:
                    mov.at[i, "Debito"] = abs(float(imp))
        else:
            amt = abs(delta)
            if delta > 0:
                mov.at[i, "Credito"] = amt
            else:
                mov.at[i, "Debito"] = amt

        prev_saldo = saldo

    mov_final = mov.drop(columns=["Tipo"], errors="ignore")

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


