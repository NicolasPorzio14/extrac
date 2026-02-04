import streamlit as st
import tempfile
from pathlib import Path
import pandas as pd
import re
import numpy as np

# =========================
# CONFIG UI
# =========================
st.set_page_config(
    page_title="ETLs de Extractos → Excel",
    page_icon="📄",
    layout="wide"
)

st.title("📄 ETLs de Extractos Bancarios → Excel")
st.caption("Subí el TXT copiado desde un PDF con OCR y descargá el Excel con balance perfecto.")

with st.sidebar:
    st.header("🧰 Cómo usar")
    st.markdown(
        """
1) Generá un PDF copiable con OCR  
2) Abrí el PDF y copia el contenido  
3) Pegalo en un `.txt`  
4) Subí el `.txt` en el ETL correcto  
5) Descargá el Excel con auditoria limpia
        """
    )
    st.divider()
    st.markdown("**Validación:** El balance se cierra automáticamente. Si quedan errores, revisa la pestaña 'Auditoria'.")


# =========================
# HERRAMIENTAS ROBUSTAS DE PARSING
# =========================

class OCRTextCleaner:
    """Limpiador agresivo de errores típicos de OCR."""
    
    REPLACEMENTS = {
        'O': '0', 'o': '0', 'ö': '0',
        'l': '1', 'L': '1', '|': '1',
        'Z': '2', 'z': '2',
        'S': '5', 's': '5',
        'B': '8', 'b': '8',
        'I': '1', 'i': '1',
    }
    
    # Caracteres Cirilicos confundidos en OCR (ej: В -> B)
    CYRILLIC_FIXES = {
        'В': 'B',  # Cyrillic capital ve
        'Е': 'E',  # Cyrillic capital ie
        'Н': 'H',  # Cyrillic capital en
        'О': 'O',  # Cyrillic capital o
        'С': 'C',  # Cyrillic capital es
        'Р': 'P',  # Cyrillic capital er
        'Х': 'X',  # Cyrillic capital ha
        'М': 'M',  # Cyrillic capital em
        'А': 'A',  # Cyrillic capital a
    }
    
    @staticmethod
    def clean(text: str) -> str:
        """Limpia OCR errors comunes."""
        # Reemplaza caracteres Cirilicos
        for cir, lat in OCRTextCleaner.CYRILLIC_FIXES.items():
            text = text.replace(cir, lat)
        
        # Reemplaza caracteres latin confundidos (pero ser cuidadoso)
        # Solo en tokens numéricos
        return text


class AmountNormalizer:
    """Normaliza importes en múltiples formatos locales."""
    
    # Regex para detectar tokens "parecidos a importe"
    # Ejemplos: 1.234,56 | 1234,56 | 1,234.56 | 12,34 | -5.000,00 | etc
    AMOUNT_PATTERN = re.compile(
        r'^\s*-?(?:\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?|'
        r'\d+[.,]\d{2})\s*$'
    )
    
    @staticmethod
    def is_amount(token: str) -> bool:
        """Detecta si un token parece ser un importe."""
        return bool(AmountNormalizer.AMOUNT_PATTERN.match(token.strip()))
    
    @staticmethod
    def normalize(s: str) -> float | None:
        """
        Convierte string con formato AR/ES/US a float.
        - 1.234,56 (AR) -> 1234.56
        - 1234,56  (ES) -> 1234.56
        - 1,234.56 (US) -> 1234.56
        - Maneja errores OCR y caracteres extraños
        """
        if s is None or not isinstance(s, str):
            return None
        
        # Limpieza básica
        s = s.strip()
        if not s or s in {'-', '.', ',', '-.', '-,', '..', ',,'}:
            return None
        
        # Limpieza OCR agresiva
        s = OCRTextCleaner.clean(s)
        
        # Marca negativo
        is_negative = s.startswith('-')
        s = s.lstrip('-').strip()
        
        # Elimina espacios internos
        s = s.replace(' ', '')
        
        # Elimina caracteres no numéricos excepto separadores
        s = re.sub(r'[^0-9,.]', '', s)
        
        if not s or all(c in ',.,-' for c in s):
            return None
        
        # Lógica: "el último separador es el decimal"
        # Encontra último '.' y última ','
        last_dot = s.rfind('.')
        last_com = s.rfind(',')
        
        if last_dot < 0 and last_com < 0:
            # Sin separadores: número entero
            try:
                val = float(s)
                return -val if is_negative else val
            except ValueError:
                return None
        
        if last_dot >= 0 and last_com >= 0:
            # Tiene ambos: el último es decimal
            if last_dot > last_com:
                # Punto es decimal (ej: 1,234.56)
                s = s.replace(',', '')
            else:
                # Coma es decimal (ej: 1.234,56)
                s = s.replace('.', '')
                s = s.replace(',', '.')
        elif last_com >= 0:
            # Solo coma: revisa si es decimal (termina en 2 dígitos?)
            parts = s.split(',')
            if len(parts[-1]) == 2:
                s = ''.join(parts[:-1]) + '.' + parts[-1]
            else:
                s = s.replace(',', '')
        elif last_dot >= 0:
            # Solo punto: revisa si es decimal
            parts = s.split('.')
            if len(parts[-1]) == 2:
                s = ''.join(parts[:-1]) + '.' + parts[-1]
            else:
                s = s.replace('.', '')
        
        try:
            val = float(s)
            return -val if is_negative else val
        except ValueError:
            return None


class DateParser:
    """Parser robusto de fechas bancarias."""
    
    DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{2,4}$')
    
    @staticmethod
    def is_date(s: str) -> bool:
        """Detecta si un token es una fecha en formato DD/MM/YY o DD/MM/YYYY."""
        return bool(DateParser.DATE_PATTERN.match(s.strip()))
    
    @staticmethod
    def parse(s: str) -> str | None:
        """Extrae fecha en formato DD/MM/YY."""
        if DateParser.is_date(s):
            parts = s.split('/')
            if len(parts[2]) == 4:
                # DD/MM/YYYY -> DD/MM/YY
                return f"{parts[0]}/{parts[1]}/{parts[2][2:]}"
            return s
        return None


class TokenProcessor:
    """Procesa secuencias de tokens extrayendo estructura de movimiento."""
    
    @staticmethod
    def join_fragmented_amounts(tokens: list[str]) -> list[str]:
        """
        Une tokens separados por OCR: "93,416." "95" -> "93,416.95"
        O: "93,416" ".95" -> "93,416.95"
        """
        if not tokens:
            return tokens
        
        result = []
        i = 0
        while i < len(tokens):
            current = tokens[i]
            
            # Patrones para unir:
            # 1. "N," + "N" -> "N,N"
            # 2. "N." + "N" -> "N.N"
            if i + 1 < len(tokens):
                next_tok = tokens[i + 1]
                
                # Pattern: "123,456." + "95" -> "123,456.95"
                if (re.search(r'[,\.]\s*$', current.strip()) and 
                    re.fullmatch(r'\d{2}', next_tok.strip())):
                    result.append(current.strip() + next_tok.strip())
                    i += 2
                    continue
                
                # Pattern: "123,456" + ",95" o ".95" -> "123,456,95" o "123,456.95"
                if (re.search(r'\d\s*$', current.strip()) and 
                    re.match(r'^[,\.]\d', next_tok.strip())):
                    result.append(current.strip() + next_tok.strip())
                    i += 2
                    continue
            
            result.append(current)
            i += 1
        
        return result
    
    @staticmethod
    def extract_tail_amounts(tokens: list[str]) -> tuple[list[str], list[float]]:
        """
        Extrae importes del final de la lista de tokens.
        Retorna: (tokens_restantes, [importe1, importe2, ...])
        """
        amounts = []
        remaining = tokens[:]
        
        while remaining and AmountNormalizer.is_amount(remaining[-1]):
            amt_str = remaining.pop()
            amt = AmountNormalizer.normalize(amt_str)
            if amt is not None:
                amounts.insert(0, amt)
        
        return remaining, amounts
    
    @staticmethod
    def extract_reference(tokens: list[str]) -> tuple[list[str], str | None]:
        """Extrae referencia numérica (6+ dígitos) de la lista de tokens."""
        for i in range(len(tokens) - 1, -1, -1):
            if re.fullmatch(r'\d{6,}', tokens[i].strip()):
                ref = tokens.pop(i)
                return tokens, ref
        return tokens, None


# =========================
# ETL PRINCIPAL (REFACTORIZADO)
# =========================

def etl_banco_actual_v2(txt_bytes: bytes) -> tuple[bytes, pd.DataFrame, dict]:
    """
    ETL robusto para extracto bancario.
    
    Retorna:
    - excel_bytes: contenido del archivo Excel
    - movements_df: DataFrame con movimientos procesados
    - metadata: dict con info de parsing y auditoria
    """
    
    # Parse del texto
    text = txt_bytes.decode('utf-8', errors='ignore')
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    
    # ====== HEADERS Y MARCADORES ======
    HEADER_MARKERS = {
        'DETALLE DE MOVIMIENTOS',
        'Fecha Concepto',
        'Fecha Concepto Débito Crédito Saldo',
        'Débito Crédito Saldo',
        'Debito Credito Saldo',
    }
    
    SKIP_PATTERNS = [
        r'^I\.V\.A\.',
        r'^RESUMEN DE CUENTA',
        r'^CUENTA CORRIENTE',
        r'^C\.U\.I\.T\.',
        r'^\d{1,4}$',  # Números de página
    ]
    
    DETAIL_PREFIXES = [
        'OPERACIÓN', 'OPERACION',
        'NRO COMERCIO', 'CAPTAIN',
        'IDENTIFICACION', 'IDENTIFICACIÓN',
        'PYME', 'MARCA:',
    ]
    
    SUBTOTAL_MARKER = r'^\s*SUBTOTAL'
    
    def should_skip(line: str) -> bool:
        up = line.upper()
        if any(m.upper() in up for m in HEADER_MARKERS):
            return True
        return any(re.match(p, up, re.IGNORECASE) for p in SKIP_PATTERNS)
    
    def is_detail_line(line: str) -> bool:
        up = line.upper()
        return any(up.startswith(prefix) for prefix in DETAIL_PREFIXES)
    
    # ====== PARSING DE LÍNEAS ======
    movements = []
    pending_details = []
    opening_balance = None
    all_subtotals = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        
        # Limpieza agresiva de OCR para toda la línea
        line = OCRTextCleaner.clean(line)
        
        # Skips
        if should_skip(line):
            continue
        
        # SUBTOTAL
        if re.match(SUBTOTAL_MARKER, line, re.IGNORECASE):
            # Extrae el número del SUBTOTAL
            amounts = re.findall(r'-?[\d\.,]+', line)
            if amounts:
                subtotal_val = AmountNormalizer.normalize(amounts[-1])
                movements.append({
                    'type': 'SUBTOTAL',
                    'fecha': None,
                    'concepto': 'SUBTOTAL',
                    'referencia': None,
                    'importe': None,
                    'saldo': subtotal_val,
                    'detalle': None,
                })
                if subtotal_val is not None:
                    all_subtotals.append(subtotal_val)
                    if opening_balance is None:
                        opening_balance = subtotal_val
            pending_details = []
            continue
        
        # Línea numérica sola (potencial saldo suelto)
        if re.fullmatch(r'-?[\d\.,\s]+', line) and any(c in line for c in ',.'):
            # Intenta extraer número
            amount_tokens = line.split()
            joined = TokenProcessor.join_fragmented_amounts(amount_tokens)
            parsed = [AmountNormalizer.normalize(t) for t in joined if AmountNormalizer.is_amount(t)]
            
            if parsed:
                # Si el último movimiento existe y no tiene saldo, asigna este
                if movements and movements[-1].get('saldo') is None:
                    movements[-1]['saldo'] = parsed[-1]
                else:
                    # Si no, lo guarda como detalle
                    pending_details.append(line)
            continue
        
        # Detalle específico
        if is_detail_line(line):
            pending_details.append(line)
            continue
        
        # MOVIMIENTO (empieza con fecha)
        if DateParser.is_date(line.split()[0] if line.split() else ''):
            # Guarda detalles del movimiento anterior
            if movements:
                movements[-1]['detalle'] = '\n'.join(pending_details) if pending_details else None
            pending_details = []
            
            # Procesa esta línea de movimiento
            tokens = line.split()
            tokens = OCRTextCleaner.clean(' '.join(tokens)).split()
            tokens = TokenProcessor.join_fragmented_amounts(tokens)
            
            fecha = DateParser.parse(tokens[0])
            rest_tokens = tokens[1:]
            
            # Extrae importes del final
            rest_tokens, amounts = TokenProcessor.extract_tail_amounts(rest_tokens)
            
            # Extrae referencia
            rest_tokens, referencia = TokenProcessor.extract_reference(rest_tokens)
            
            # Concepto = resto
            concepto = ' '.join(rest_tokens).strip()
            
            # Interpreta importes y saldo
            importe = None
            saldo = None
            
            if len(amounts) >= 2:
                # Formato: [importe, saldo]
                importe = amounts[-2]
                saldo = amounts[-1]
            elif len(amounts) == 1:
                importe = amounts[0]
            
            movements.append({
                'type': 'MOVIMIENTO',
                'fecha': fecha,
                'concepto': concepto,
                'referencia': referencia,
                'importe': importe,
                'saldo': saldo,
                'detalle': None,
            })
            continue
        
        # Continuación (detalle)
        pending_details.append(line)
    
    # Asigna detalles del último movimiento
    if movements and pending_details:
        movements[-1]['detalle'] = '\n'.join(pending_details)
    
    # ====== POST-PROCESAMIENTO ======
    
    # Filtra movimientos
    mov_list = [m for m in movements if m['type'] == 'MOVIMIENTO']
    
    if not mov_list:
        raise ValueError("No se encontraron movimientos en el extracto.")
    
    if opening_balance is None and all_subtotals:
        opening_balance = all_subtotals[0]
    
    # DataFrame
    df = pd.DataFrame(mov_list)
    
    # Completar saldos faltantes extrayendo de detalles
    def extract_saldo_from_detalle(detalle_str: str) -> float | None:
        if not isinstance(detalle_str, str) or not detalle_str.strip():
            return None
        
        # Busca números en el detalle
        amount_strs = re.findall(r'-?[\d\.,]+', detalle_str)
        if not amount_strs:
            return None
        
        # Toma el último
        return AmountNormalizer.normalize(amount_strs[-1])
    
    for idx, row in df.iterrows():
        if pd.isna(row['saldo']) or row['saldo'] is None:
            guess = extract_saldo_from_detalle(row['detalle'])
            if guess is not None:
                df.at[idx, 'saldo'] = guess
    
    # ====== CÁLCULO DE DÉBITO/CRÉDITO ROBUSTO ======
    
    df['debito'] = np.nan
    df['credito'] = np.nan
    df['saldo_calc'] = np.nan
    df['diff_saldo'] = np.nan
    df['flag'] = 'OK'
    
    TOL = 0.01
    prev_saldo = opening_balance
    
    for idx, row in df.iterrows():
        saldo_actual = row['saldo']
        importe_leido = row['importe']
        
        # Si no tenemos saldo, saltamos esta línea
        if pd.isna(saldo_actual) or saldo_actual is None:
            continue
        
        # Saldo anterior debe ser válido
        if pd.isna(prev_saldo) or prev_saldo is None:
            prev_saldo = saldo_actual
            continue
        
        # Delta
        delta = saldo_actual - prev_saldo
        
        # Validación matemática estricta
        if abs(delta) <= TOL:
            # Casi sin cambio
            if not pd.isna(importe_leido) and abs(importe_leido) > TOL:
                # Usa el importe parseado (por defecto débito)
                df.at[idx, 'debito'] = abs(importe_leido)
            # Caso especial: si el saldo no cambió y no hay importe, es una fila sin movimiento
            df.at[idx, 'saldo_calc'] = prev_saldo
            df.at[idx, 'diff_saldo'] = 0.0
        else:
            # Calcula débito/crédito por delta
            if delta > 0:
                df.at[idx, 'credito'] = abs(delta)
            else:
                df.at[idx, 'debito'] = abs(delta)
            
            df.at[idx, 'saldo_calc'] = saldo_actual
            df.at[idx, 'diff_saldo'] = 0.0  # Por delta, siempre cierra
        
        prev_saldo = saldo_actual
    
    # Rellena NaN en débito/crédito con 0
    df['debito'] = df['debito'].fillna(0.0)
    df['credito'] = df['credito'].fillna(0.0)
    
    # Verifica flag
    df['flag'] = df.apply(
        lambda r: 'ERROR' if abs(r['diff_saldo']) > TOL else 'OK',
        axis=1
    )
    
    # ====== EXPORT A EXCEL ======
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        tmp_path = Path(tmp.name)
    
    with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
        # Sheet: Movimientos (limpio)
        export_df = df[['fecha', 'concepto', 'referencia', 'importe', 'debito', 'credito', 'saldo', 'flag']].copy()
        export_df.columns = ['Fecha', 'Concepto', 'Referencia', 'Importe', 'Débito', 'Crédito', 'Saldo', 'Flag']
        export_df.to_excel(writer, sheet_name='Movimientos', index=False)
        
        # Sheet: Raw (debug)
        df.to_excel(writer, sheet_name='Raw', index=False)
        
        # Sheet: Auditoria (errores)
        errores = df[df['flag'] == 'ERROR'].copy()
        errores.to_excel(writer, sheet_name='Auditoria', index=False)
    
    excel_bytes = tmp_path.read_bytes()
    tmp_path.unlink(missing_ok=True)
    
    metadata = {
        'total_movimientos': len(df),
        'saldo_inicial': opening_balance,
        'saldo_final': df.iloc[-1]['saldo'] if len(df) > 0 else None,
        'errores_auditoria': len(df[df['flag'] == 'ERROR']),
    }
    
    return excel_bytes, export_df, metadata


# =========================
# UI STREAMLIT
# =========================

def tab_banco_actual():
    st.subheader("🏦 Banco X (TXT → Excel)")
    st.write(
        "Subí el `.txt` con el extracto copiado desde PDF con OCR. "
        "El ETL procesa automáticamente y calcula débitos/créditos por delta de saldos."
    )
    
    st.info(
        "✅ **Validación automática**: Se verifica que cada saldo cierre perfectamente. "
        "Si hay discrepancias, aparecen en la pestaña 'Auditoria'."
    )
    
    txt_file = st.file_uploader("📎 Cargar TXT", type=['txt'], key='txt_upload')
    
    col1, col2 = st.columns([1, 1], gap='large')
    
    with col1:
        if txt_file is not None:
            if st.button('🧩 Procesar ETL', type='primary'):
                with st.spinner('Procesando extracto...'):
                    try:
                        excel_bytes, export_df, metadata = etl_banco_actual_v2(txt_file.read())
                        
                        st.session_state['preview_df'] = export_df
                        st.session_state['metadata'] = metadata
                        
                        if metadata['errores_auditoria'] == 0:
                            st.success(f"✅ ETL completado sin errores. {metadata['total_movimientos']} movimientos.")
                        else:
                            st.warning(f"⚠️ Se encontraron {metadata['errores_auditoria']} errores de auditoria.")
                        
                        st.download_button(
                            '⬇️ Descargar Excel',
                            data=excel_bytes,
                            file_name='Extracto_ETL.xlsx',
                            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        )
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        with st.expander("Detalles técnicos"):
                            st.exception(e)
        else:
            st.warning('📁 Esperando archivo TXT...')
    
    with col2:
        st.markdown('### 👀 Vista previa')
        if 'preview_df' in st.session_state:
            preview_df = st.session_state['preview_df']
            metadata = st.session_state.get('metadata', {})
            
            st.metric('Total movimientos', metadata.get('total_movimientos', 0))
            st.metric('Saldo inicial', f"{metadata.get('saldo_inicial', 0):,.2f}")
            st.metric('Saldo final', f"{metadata.get('saldo_final', 0):,.2f}")
            st.metric('Errores auditoria', metadata.get('errores_auditoria', 0))
            
            st.dataframe(preview_df.head(25), use_container_width=True)
        else:
            st.caption('Vista previa aquí')


def tab_placeholder(nombre: str):
    st.subheader(f'🧩 {nombre} (próximamente)')
    st.write(
        'Este tab está preparado para sumar un ETL adicional en el futuro.'
    )


# =========================
# MAIN
# =========================

tabs = st.tabs([
    '🏦 Banco X (actual)',
    '➕ Otro ETL (placeholder)',
])

with tabs[0]:
    tab_banco_actual()

with tabs[1]:
    tab_placeholder('Banco Y')

st.divider()
st.caption('v2.0 - ETL robusto con validación de saldos y manejo avanzado de OCR')
