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

    # =========================
    # Regex / helpers
    # =========================
    date_re = re.compile(r"^\d{2}/\d{2}/\d{2}\b")

    HEADER_MARKERS = {
        "DETALLE DE MOVIMIENTOS",
        "Fecha Concepto",
        "Fecha Concepto Débito Crédito Saldo",
        "Débito Crédito Saldo",
        "Debito Credito Saldo",
        "Fecha Concepto Débito Crédito",
    }

    def is_header(line: str) -> bool:
        up = line.upper().strip()
        if up in {h.upper() for h in HEADER_MARKERS}:
            return True
        if up.startswith(("I.V.A.", "RESUMEN DE CUENTA", "CUENTA CORRIENTE", "C.U.I.T.")) or "C.U.I.T." in up:
            return True
        return False

    # Token de importe "real": tiene separadores y termina en 2 decimales (coma o punto)
    # Ej: 1.234,56 | 1234,56 | -12.345,67 | 12,34 | 12.34
    amount_token_re = re.compile(r"^-?\d{1,3}([.,]\d{3})*([.,]\d{2})$|^-?\d+([.,]\d{2})$")

    def is_amount_token(tok: str) -> bool:
        tok = tok.strip()
        return bool(amount_token_re.fullmatch(tok))

    def normalize_amount(s: str):
        """
        Convierte string con formato AR/ES a float:
        1.234,56 -> 1234.56
        1234,56  -> 1234.56
        1,234.56 -> 1234.56 (si apareciera)
        """
        if s is None:
            return None
        s = s.strip().replace(" ", "")
        if not s:
            return None

        # Limpieza OCR típica
        s = s.replace("O", "0").replace("o", "0")
        s = re.sub(r"[^0-9\-,\.]", "", s)
        if s in {"", "-", ".", ",", "-.", "-,"}:
            return None

        neg = s.startswith("-")
        s2 = s[1:] if neg else s

        # Caso con ambos separadores: decide decimal por el último separador
        if "." in s2 and "," in s2:
            last_dot = s2.rfind(".")
            last_com = s2.rfind(",")
            dec = "." if last_dot > last_com else ","
            thou = "," if dec == "." else "."
            s2 = s2.replace(thou, "")
            if dec == ",":
                s2 = s2.replace(",", ".")
        else:
            # Solo coma -> decimal si termina en 2 dígitos
            if "," in s2:
                parts = s2.split(",")
                if len(parts[-1]) == 2:
                    s2 = "".join(parts[:-1]) + "." + parts[-1]
                else:
                    s2 = s2.replace(",", "")
            # Solo punto -> decimal si termina en 2 dígitos
            elif "." in s2:
                parts = s2.split(".")
                if len(parts[-1]) == 2:
                    s2 = "".join(parts[:-1]) + "." + parts[-1]
                else:
                    s2 = s2.replace(".", "")

        try:
            v = float(s2)
            return -v if neg else v
        except:
            return None

    def join_split_decimals(tokens):
        """
        Une tokens que OCR partió:
        "93,416." "95" => "93,416.95"
        "93,416" ".95" => "93,416.95"
        """
        out = []
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if i + 1 < len(tokens):
                n = tokens[i + 1]
                if re.fullmatch(r"-?[\d\.,]+[.,]$", t) and re.fullmatch(r"\d{2}$", n):
                    out.append(t + n)
                    i += 2
                    continue
                if re.fullmatch(r"-?[\d\.,]+$", t) and re.fullmatch(r"[.,]\d{2}$", n):
                    out.append(t + n)
                    i += 2
                    continue
            out.append(t)
            i += 1
        return out

    def extract_tail_amounts(tokens):
        toks = join_split_decimals(tokens)
        rest = toks[:]
        amts = []
        while rest and is_amount_token(rest[-1]):
            amts.append(rest.pop())
        amts.reverse()
        return rest, amts

    # =========================
    # Parse TXT
    # =========================
    text = txt_bytes.decode("utf-8", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    rows = []
    pending_detail = []
    last_movement_idx = None

    # Para asociar saldos “en línea siguiente”
    expecting_saldo_for_last = False

    # Apertura / subtotales como checkpoints
    opening_saldo = None
    subtotals = []

    def attach_detail(idx):
        nonlocal pending_detail
        if idx is None or not pending_detail:
            return
        prev = rows[idx].get("Detalle") or ""
        add = "\n".join(pending_detail)
        rows[idx]["Detalle"] = (prev + ("\n" if prev else "") + add).strip()
        pending_detail.clear()

    for i, ln in enumerate(lines):
        up = ln.upper()

        if is_header(ln):
            continue

        # Página (número solo)
        if re.fullmatch(r"\d{1,4}", ln):
            continue

        # SUBTOTAL con número real
        if up.startswith("SUBTOTAL") and re.search(r"\d", ln):
            nums = re.findall(r"-?[\d\.,]+", ln)
            st = normalize_amount(nums[-1]) if nums else None
            rows.append({
                "Tipo": "SUBTOTAL",
                "Fecha": None,
                "Concepto": "SUBTOTAL",
                "Referencia": None,
                "Importe_Parsed": None,
                "Saldo": st,
                "Detalle": None
            })
            if st is not None:
                subtotals.append(st)
                if opening_saldo is None:
                    opening_saldo = st
            last_movement_idx = None
            continue

        # “Saldo” como marcador: el número de la próxima línea es saldo del último movimiento
        if up == "SALDO":
            expecting_saldo_for_last = True
            continue

        # Detalles típicos
        if up.startswith(("OPERACIÓN", "OPERACION", "NRO COMERCIO", "CAPTAIN", "PYME", "IDENTIFICACION", "IDENTIFICACIÓN")):
            pending_detail.append(ln)
            continue

        # Línea NUMÉRICA sola
        if re.fullmatch(r"-?[\d\.,]+", ln) and any(ch in ln for ch in [",", "."]):
            val = normalize_amount(ln)

            # Si justo venía un “Saldo” -> asigno saldo al último movimiento
            if val is not None and expecting_saldo_for_last and last_movement_idx is not None:
                rows[last_movement_idx]["Saldo"] = val
                expecting_saldo_for_last = False
                continue

            # Si no era saldo, lo dejo como detalle (puede ser “importe suelto” por OCR)
            expecting_saldo_for_last = False
            pending_detail.append(ln)
            continue

        # Movimiento con fecha
        if date_re.match(ln):
            attach_detail(last_movement_idx)

            tokens = join_split_decimals(ln.split())
            fecha = tokens[0]
            rest, amt_tokens = extract_tail_amounts(tokens[1:])

            saldo = None
            mov_amt = None

            # Si hay 2 importes al final: [importe, saldo]
            if len(amt_tokens) >= 2:
                mov_amt = normalize_amount(amt_tokens[-2])
                saldo = normalize_amount(amt_tokens[-1])
            elif len(amt_tokens) == 1:
                mov_amt = normalize_amount(amt_tokens[0])

            # Referencia numérica si existe
            referencia = None
            for t in reversed(rest):
                if re.fullmatch(r"\d{6,}", t):
                    referencia = t
                    break

            concepto_tokens = rest[:]
            if referencia and referencia in concepto_tokens:
                for k in range(len(concepto_tokens) - 1, -1, -1):
                    if concepto_tokens[k] == referencia:
                        concepto_tokens.pop(k)
                        break

            concepto = " ".join(concepto_tokens).strip()

            rows.append({
                "Tipo": "MOVIMIENTO",
                "Fecha": fecha,
                "Concepto": concepto,
                "Referencia": referencia,
                "Importe_Parsed": mov_amt,
                "Saldo": saldo,
                "Detalle": None
            })

            last_movement_idx = len(rows) - 1
            expecting_saldo_for_last = False
            continue

        # Continuación no-fecha: lo guardo como detalle
        pending_detail.append(ln)

    attach_detail(last_movement_idx)

    raw_df = pd.DataFrame(rows)

    # =========================
    # Post-proceso: movimientos
    # =========================
    mov = raw_df[raw_df["Tipo"] == "MOVIMIENTO"].copy().reset_index(drop=True)

    for c in ["Importe_Parsed", "Saldo"]:
        mov[c] = pd.to_numeric(mov[c], errors="coerce")

    # --- PASO CLAVE: completar saldos faltantes usando saldos "candidatos" que quedaron en Detalle ---
    # Heurística: si un movimiento no tiene saldo, pero su Detalle contiene un número "con decimales",
    # tomo el ÚLTIMO número de detalle como saldo probable.
    def extract_last_amount_from_detail(detail: str):
        if not isinstance(detail, str) or not detail.strip():
            return None
        # busco tokens "parecidos a importe"
        toks = join_split_decimals(detail.replace("\n", " ").split())
        candidates = [t for t in toks if is_amount_token(t)]
        if not candidates:
            # fallback: casos donde OCR deja "873,519.92" como token válido pero nuestro regex no lo capturó:
            # (igual debería, pero por las dudas)
            candidates = re.findall(r"-?\d[\d\.,]*\d", detail)
        if not candidates:
            return None
        val = normalize_amount(candidates[-1])
        return val

    for i in range(len(mov)):
        if pd.isna(mov.at[i, "Saldo"]):
            det = mov.at[i, "Detalle"] if "Detalle" in mov.columns else None
            guess = extract_last_amount_from_detail(det)
            if guess is not None:
                mov.at[i, "Saldo"] = guess

    # =========================
    # Débito / Crédito por delta (garantiza cierre si saldos están bien)
    # =========================
    mov["Debito"] = np.nan
    mov["Credito"] = np.nan

    tol = 0.01

    prev = opening_saldo
    for i in range(len(mov)):
        saldo = mov.at[i, "Saldo"]

        if pd.isna(saldo) or prev is None or (isinstance(prev, float) and np.isnan(prev)):
            prev = saldo if not pd.isna(saldo) else prev
            continue

        delta = saldo - prev

        if abs(delta) <= tol:
            # casi 0: si hay importe parseado, uso eso
            imp = mov.at[i, "Importe_Parsed"]
            if not pd.isna(imp) and abs(float(imp)) > tol:
                # Por defecto: si no hay forma de saber, lo pongo como débito
                mov.at[i, "Debito"] = abs(float(imp))
            prev = saldo
            continue

        if delta > 0:
            mov.at[i, "Credito"] = abs(delta)
        else:
            mov.at[i, "Debito"] = abs(delta)

        prev = saldo

    # =========================
    # Auditoría de cierre
    # =========================
    mov["Debito"] = mov["Debito"].fillna(0.0)
    mov["Credito"] = mov["Credito"].fillna(0.0)

    mov["Saldo_calc"] = opening_saldo + (mov["Credito"] - mov["Debito"]).cumsum()
    mov["Diff_Saldo"] = (mov["Saldo"] - mov["Saldo_calc"]).round(2)

    mov["Flag"] = np.where(mov["Diff_Saldo"].abs() > 0.01, "ERROR", "OK")

    # =========================
    # Export Excel
    # =========================
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        mov.to_excel(writer, sheet_name="Movimientos", index=False)
        raw_df.to_excel(writer, sheet_name="Raw_Parse", index=False)
        mov[mov["Flag"] == "ERROR"].to_excel(writer, sheet_name="Auditoria", index=False)

    excel_bytes = tmp_path.read_bytes()
    tmp_path.unlink(missing_ok=True)

    return excel_bytes, mov, raw_df


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



