import streamlit as st
import pandas as pd
import numpy as np
import re
from pathlib import Path
import tempfile

st.set_page_config(page_title="ETL Extractos Bancarios", page_icon="📄", layout="wide")

st.title("📄 ETL Extractos Bancarios → Excel CORRECTO")
st.caption("Procesa TXT (desde PDF+OCR) a Excel con balance 100% exacto")

# =========================
# NORMALIZACIÓN DE NÚMEROS
# =========================

def normalize_number(s: str) -> float | None:
    """Convierte texto con múltiples formatos a float"""
    if not s or not isinstance(s, str):
        return None
    
    s = s.strip()
    
    # Limpieza OCR: reemplaza caracteres confundidos
    replacements = {
        'O': '0', 'o': '0', 'ö': '0',
        'l': '1', 'L': '1', '|': '1', 'I': '1',
        'Z': '2', 'z': '2', 'S': '5',
        'B': '8', 'b': '8',
        # Cirilicos
        'В': 'B', 'Е': 'E', 'Н': 'H', 'О': 'O',
        'С': 'C', 'Р': 'P', 'Х': 'X', 'М': 'M', 'А': 'A',
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    
    # Elimina caracteres no-numéricos excepto separadores
    s = re.sub(r'[^\d\.\,-]', '', s)
    
    if not s or all(c in ',.,-' for c in s):
        return None
    
    # Marca negativo
    is_neg = s.startswith('-')
    s = s.lstrip('-').strip()
    
    # Lógica: último separador es el decimal
    last_dot = s.rfind('.')
    last_com = s.rfind(',')
    
    if last_dot < 0 and last_com < 0:
        try:
            v = float(s)
            return -v if is_neg else v
        except:
            return None
    
    if last_dot >= 0 and last_com >= 0:
        # Ambos: el último es decimal
        if last_dot > last_com:
            s = s.replace(',', '')
        else:
            s = s.replace('.', '')
            s = s.replace(',', '.')
    elif last_com >= 0:
        parts = s.split(',')
        if len(parts[-1]) == 2:
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        else:
            s = s.replace(',', '')
    elif last_dot >= 0:
        parts = s.split('.')
        if len(parts[-1]) == 2:
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        else:
            s = s.replace('.', '')
    
    try:
        v = float(s)
        return -v if is_neg else v
    except:
        return None


def extract_numbers_from_line(line: str) -> list[float]:
    """Extrae todos los números de una línea en orden"""
    # Patrón para capturar números con formatos múltiples
    pattern = r'-?[\d]{1,3}(?:[.,][\d]{3})*(?:[.,][\d]{2})?|-?[\d]+[.,][\d]{1,2}'
    matches = re.findall(pattern, line)
    
    numbers = []
    for match in matches:
        num = normalize_number(match)
        if num is not None:
            numbers.append(num)
    
    return numbers


# =========================
# PARSER PRINCIPAL
# =========================

def parse_extracto(txt_bytes: bytes) -> pd.DataFrame:
    """
    Parser para extracto bancario argentino
    
    Estructura esperada:
    DD/MM/YY Concepto Referencia IMPORTE SALDO
    DD/MM/YY Concepto Referencia Nro-Ref-6+ IMPORTE SALDO
    
    - El PENÚLTIMO número es el IMPORTE (lo que movió)
    - El ÚLTIMO número es el SALDO (resultado acumulado)
    - Débito/Crédito se calcula por diferencia de saldos
    """
    
    text = txt_bytes.decode('utf-8', errors='ignore')
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    
    # Keywords que indican skip
    skip_patterns = [
        r'^DETALLE DE MOVIMIENTOS',
        r'^Fecha Concepto',
        r'^Débito Crédito',
        r'^Crédito Débito',
        r'^I\.V\.A\.',
        r'^RESUMEN DE CUENTA',
        r'^CUENTA CORRIENTE',
        r'^C\.U\.I\.T\.',
        r'^PYME',
        r'^CAPTAIN',
        r'^Nro Comercio',
        r'^IDENTIFICACION',
        r'^IDENTIFICACIÓN',
        r'^Operación',
        r'^OPERACION',
        r'^Marca:',
        r'^IMP\.AFIP',
    ]
    
    def should_skip(line: str) -> bool:
        # Número de página
        if re.fullmatch(r'\d{1,4}', line):
            return True
        # Keywords
        for pattern in skip_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                return True
        return False
    
    def has_date(line: str) -> bool:
        """Detecta si línea comienza con fecha DD/MM/YY"""
        return bool(re.match(r'\d{2}/\d{2}/\d{2,4}', line))
    
    movements = []
    saldo_inicial = None
    
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        
        if should_skip(line):
            continue
        
        # SUBTOTAL (marca inicial de saldo)
        if 'SUBTOTAL' in line.upper():
            nums = extract_numbers_from_line(line)
            if nums:
                saldo_inicial = nums[-1]
            continue
        
        # LÍNEA CON MOVIMIENTO (comienza con fecha)
        if has_date(line):
            numbers = extract_numbers_from_line(line)
            
            # Necesita al menos 2 números (importe y saldo)
            if len(numbers) < 2:
                continue
            
            # Penúltimo = importe, Último = saldo
            importe = numbers[-2]
            saldo = numbers[-1]
            
            # Extrae fecha
            fecha_match = re.match(r'(\d{2}/\d{2}/\d{2,4})', line)
            fecha = fecha_match.group(1) if fecha_match else None
            
            # Extrae referencia (6+ dígitos consecutivos)
            ref_match = re.search(r'\b(\d{6,})\b', line)
            referencia = ref_match.group(1) if ref_match else None
            
            # Concepto: todo lo que está entre fecha y los números
            concepto = line[len(fecha):].strip() if fecha else line.strip()
            
            # Quita los números del concepto (para dejarlo limpio)
            for num_str in [str(n) for n in numbers]:
                concepto = re.sub(r'\b' + re.escape(num_str) + r'\b', '', concepto)
            
            concepto = concepto.strip()
            
            movements.append({
                'fecha': fecha,
                'concepto': concepto,
                'referencia': referencia,
                'importe': importe,
                'saldo': saldo,
            })
    
    if not movements:
        raise ValueError("❌ No se encontraron movimientos en el archivo")
    
    df = pd.DataFrame(movements)
    
    # ===== CÁLCULO DE DÉBITO/CRÉDITO =====
    
    df['debito'] = 0.0
    df['credito'] = 0.0
    
    # Si no tenemos saldo inicial, lo deducimos del primero
    if saldo_inicial is None:
        saldo_inicial = df.iloc[0]['saldo'] - df.iloc[0]['importe']
    
    prev_saldo = saldo_inicial
    
    for idx, row in df.iterrows():
        saldo = row['saldo']
        importe = row['importe']
        concepto = row['concepto'].upper()
        
        # Delta respecto al saldo anterior
        delta = saldo - prev_saldo
        
        # Determina débito o crédito
        # Opción 1: Mira si el concepto dice /CR o /DB
        if '/CR' in concepto:
            df.at[idx, 'credito'] = importe
        elif '/DB' in concepto or '/DВ' in concepto:  # DВ es cirilico
            df.at[idx, 'debito'] = importe
        elif 'CREDITO' in concepto:
            df.at[idx, 'credito'] = importe
        elif 'DÉBITO' in concepto or 'DEBITO' in concepto:
            df.at[idx, 'debito'] = importe
        # Opción 2: Por el delta de saldos
        else:
            if delta > 0:
                df.at[idx, 'credito'] = importe
            else:
                df.at[idx, 'debito'] = importe
        
        prev_saldo = saldo
    
    # Validación
    df['saldo_calc'] = saldo_inicial + (df['credito'] - df['debito']).cumsum()
    df['diff'] = abs(df['saldo'] - df['saldo_calc'])
    df['flag'] = df['diff'].apply(lambda x: 'OK' if x < 0.01 else 'ERROR')
    
    return df


# =========================
# INTERFAZ
# =========================

col1, col2 = st.columns([1, 1], gap='large')

with col1:
    st.subheader("📥 Cargar Archivo")
    
    txt_file = st.file_uploader("Selecciona el archivo .txt", type=['txt'])
    
    if txt_file:
        if st.button('⚙️ PROCESAR', type='primary', use_container_width=True):
            with st.spinner('Analizando extracto...'):
                try:
                    df = parse_extracto(txt_file.read())
                    
                    st.session_state['df'] = df
                    
                    # Estadísticas
                    total_mov = len(df)
                    saldo_final = df.iloc[-1]['saldo'] if len(df) > 0 else 0
                    errores = len(df[df['flag'] == 'ERROR'])
                    
                    if errores == 0:
                        st.success(f'✅ Procesados {total_mov} movimientos SIN ERRORES')
                    else:
                        st.warning(f'⚠️ {total_mov} movimientos, {errores} con error')
                    
                    # Genera Excel
                    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                        tmp_path = Path(tmp.name)
                    
                    with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                        # Sheet principal
                        export_cols = ['fecha', 'concepto', 'referencia', 'importe', 'debito', 'credito', 'saldo', 'flag']
                        export_df = df[export_cols].copy()
                        export_df.columns = ['Fecha', 'Concepto', 'Referencia', 'Importe', 'Débito', 'Crédito', 'Saldo', 'Flag']
                        export_df.to_excel(writer, sheet_name='Movimientos', index=False)
                        
                        # Sheet debug
                        df.to_excel(writer, sheet_name='Debug', index=False)
                        
                        # Sheet errores
                        errores_df = df[df['flag'] == 'ERROR']
                        if len(errores_df) > 0:
                            errores_df.to_excel(writer, sheet_name='Errores', index=False)
                    
                    excel_bytes = tmp_path.read_bytes()
                    tmp_path.unlink()
                    
                    st.download_button(
                        '⬇️ DESCARGAR EXCEL',
                        data=excel_bytes,
                        file_name='Extracto_ETL.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        use_container_width=True
                    )
                    
                except Exception as e:
                    st.error(f'❌ Error: {str(e)}')
                    st.exception(e)
    else:
        st.info('👆 Carga un archivo .txt para comenzar')

with col2:
    st.subheader("📊 Vista Previa")
    
    if 'df' in st.session_state:
        df = st.session_state['df']
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.metric('Total movimientos', len(df))
            st.metric('Saldo inicial', f"${df.iloc[0]['saldo'] - df.iloc[0]['importe'] - df.iloc[0]['debito'] + df.iloc[0]['credito']:,.2f}")
        
        with col_b:
            st.metric('Saldo final', f"${df.iloc[-1]['saldo']:,.2f}")
            st.metric('Errores', len(df[df['flag'] == 'ERROR']))
        
        st.dataframe(
            df[['fecha', 'concepto', 'importe', 'debito', 'credito', 'saldo', 'flag']].head(30),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.caption('La vista previa aparecerá aquí')

st.divider()
st.caption('ETL v3.0 - Procesamiento correcto basado en análisis del formato real')
