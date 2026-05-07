"""
GGIV UPDATE TOOL — v3.0
========================
Vault Algorithm — Modulo di Aggiornamento Google Sheets

MIGLIORAMENTI v3.0 rispetto a v2:
  - GES calcolato in DUE PASSAGGI: prima raccoglie tutti i brevetti,
    poi calcola Pat_max reale e ricalcola GES con normalizzazione corretta
  - Modalità DRY RUN: visualizza anteprima di tutti i valori senza scrivere
  - Aggiornamento SELETTIVO: puoi scegliere quali colonne aggiornare
  - Crea automaticamente le colonne mancanti nel foglio
  - Aggiunge colonna Rev_Grafene_Pct se non esiste (per inserimento manuale)
  - Progress bar con stima tempo rimanente
  - Riassunto finale con tabella di tutti i valori scritti
  - Gestione robusta ticker OTC, ASX, London (.L), Xetra (.DE)

STRUTTURA GOOGLE SHEET attesa:
  Foglio "Database":
    Ticker, Azienda, Tier, Peso_Base, Data_Ultima_News,
    Market_Cap_USD, ADTV_3M_USD, Free_Float_Pct,
    Rev_Grafene_Pct,              ← NUOVO (inserimento manuale o stima)
    Brevetti_Granted, Brevetti_Pending, GES_Score,
    Flag_Ammissione, Flag_Delisting

  Foglio "Watchlist":
    Ticker, Azienda, Tier, Data_Ultima_News,
    Market_Cap_USD, ADTV_3M_USD, Free_Float_Pct,
    Flag_Ammissione, Flag_Delisting
"""

import streamlit as st
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════
# CONFIGURAZIONE
# ══════════════════════════════════════════════════════════════

VERSIONE         = "3.0"
NOME_FILE_GOOGLE = "GGIV_Database"
FOGLIO_DB        = "Database"
FOGLIO_WL        = "Watchlist"

# Soglie filtri ammissione (Rulebook sezione 2)
MIN_MARKET_CAP_USD = 10_000_000
MIN_ADTV_USD       = 250_000
MIN_FREE_FLOAT_PCT = 15.0

# Coefficienti GES (Rulebook v1.3 — Sez. 4-BIS)
GES_COEFFICIENTI = {
    "Tier 1": {"alpha": 0.30, "beta": 0.70, "psi": 1.5},
    "Tier 2": {"alpha": 0.55, "beta": 0.45, "psi": 1.0},
    "Tier 3": {"alpha": 0.70, "beta": 0.30, "psi": 0.5},
}

# Stima Rev_Grafene_Pct automatica per Tier (Rulebook Sez. 4.2)
STIMA_REV_PER_TIER = {
    "Tier 1": 0.05,
    "Tier 2": 0.30,
    "Tier 3": 0.02,
}

SUFFIX_ASHARE = [".SS", ".SZ"]

# Colonne obbligatorie del foglio Database con valori default
COLONNE_DB = {
    "Data_Ultima_News":  "",
    "Market_Cap_USD":    "",
    "ADTV_3M_USD":       "",
    "Free_Float_Pct":    "",
    "Rev_Grafene_Pct":   "",
    "Brevetti_Granted":  "0",
    "Brevetti_Pending":  "0",
    "GES_Score":         "0",
    "Flag_Ammissione":   "WARN — non verificato",
    "Flag_Delisting":    "OK",
}

COLONNE_WL = {
    "Data_Ultima_News": "",
    "Market_Cap_USD":   "",
    "ADTV_3M_USD":      "",
    "Free_Float_Pct":   "",
    "Flag_Ammissione":  "WARN — non verificato",
    "Flag_Delisting":   "OK",
}

# ══════════════════════════════════════════════════════════════
# UI HEADER
# ══════════════════════════════════════════════════════════════

st.set_page_config(page_title="GGIV Update Tool", page_icon="⬡", layout="wide")
st.markdown("""
<style>
    html, body, [data-testid="stAppViewContainer"] { background-color: #0a0e1a !important; color: #e8eaf0 !important; }
    [data-testid="stMain"], .main, .block-container { background-color: #0a0e1a !important; }
    [data-testid="stSidebar"] { background-color: #0d1b2a !important; }
    h1, h2, h3 { color: #c9a84c !important; }
    [data-testid="stMetric"] { background-color: #0d1b2a !important; border: 1px solid #1a2d45 !important; border-radius: 6px !important; padding: 12px !important; }
    .stButton > button { background-color: #0d1b2a !important; color: #00d4aa !important; border: 1px solid #00d4aa !important; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div style="font-family:'Courier New',monospace; margin-bottom:8px;">
    <span style="font-size:24px; font-weight:bold; color:#c9a84c; letter-spacing:0.15em;">⬡ GGIV UPDATE TOOL</span>
    <span style="font-size:12px; color:#7a8fa6; margin-left:12px;">v3.0 — Vault Algorithm</span>
</div>
""", unsafe_allow_html=True)
st.caption("Popola Market Cap, ADTV, Free Float, Brevetti USPTO e GES Score nel Google Sheet.")
st.markdown("---")

# ══════════════════════════════════════════════════════════════
# 1. CONNESSIONE GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════

@st.cache_resource(ttl=3600)
def connetti_sheets():
    creds_dict = dict(st.secrets["gcp_service_account"])
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open(NOME_FILE_GOOGLE)

try:
    sh = connetti_sheets()
    st.success(f"✅ Connesso a: **{NOME_FILE_GOOGLE}**")
except Exception as e:
    st.error(f"❌ ERRORE CONNESSIONE: {e}")
    st.stop()

# ══════════════════════════════════════════════════════════════
# 2. OPZIONI RUN
# ══════════════════════════════════════════════════════════════

st.markdown("### ⚙️ Opzioni")
col_opt1, col_opt2, col_opt3 = st.columns(3)

with col_opt1:
    fogli_da_aggiornare = st.multiselect(
        "Fogli da aggiornare:",
        [FOGLIO_DB, FOGLIO_WL],
        default=[FOGLIO_DB],
    )

with col_opt2:
    modalita = st.radio(
        "Modalità:",
        ["🔴 LIVE — scrivi su Sheets", "🟡 DRY RUN — solo anteprima"],
        index=0,
    )
    dry_run = "DRY RUN" in modalita

with col_opt3:
    colonne_da_aggiornare = st.multiselect(
        "Colonne da aggiornare:",
        ["Tutte", "Solo MC/ADTV/Float", "Solo GES/Brevetti", "Solo News/Flags"],
        default=["Tutte"],
    )

st.markdown("---")

# ══════════════════════════════════════════════════════════════
# 3. FUNZIONI DI SUPPORTO
# ══════════════════════════════════════════════════════════════

def safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        s = str(val).strip()
        if not s or s.startswith("#"):
            return default
        return float(s.replace("%", "").replace(",", ""))
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0) -> int:
    return int(safe_float(val, default))

def is_ashare(ticker: str) -> bool:
    return any(ticker.upper().endswith(s) for s in SUFFIX_ASHARE)

def idx_to_col_letter(idx: int) -> str:
    """Converte indice 1-based in lettera colonna Google Sheets (A, B, …, AA, AB…)."""
    result = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        result = chr(65 + rem) + result
    return result

def assicura_colonne(ws, colonne_richieste: dict) -> dict:
    """
    Verifica che tutte le colonne richieste esistano nel foglio.
    Se mancano, le aggiunge in fondo con header corretto.
    Restituisce mappa {nome_colonna: indice_1based}.
    """
    intestazioni = ws.row_values(1)
    mappa = {}
    da_aggiungere = []

    for nome in colonne_richieste:
        if nome in intestazioni:
            mappa[nome] = intestazioni.index(nome) + 1
        else:
            da_aggiungere.append(nome)

    if da_aggiungere:
        nuovi_idx = []
        for nome in da_aggiungere:
            nuovo_idx = len(intestazioni) + 1
            col_letter = idx_to_col_letter(nuovo_idx)
            ws.update(f"{col_letter}1", [[nome]])
            mappa[nome] = nuovo_idx
            intestazioni.append(nome)
            nuovi_idx.append(nome)
            time.sleep(0.3)
        st.info(f"📋 Colonne create automaticamente: {', '.join(nuovi_idx)}")

    return mappa


def get_dati_yahoo(ticker: str) -> dict:
    """
    Scarica da Yahoo Finance: Market Cap, ADTV 3M, Free Float, Data news, Delisting.
    Gestisce ticker OTC, ASX, London (.L), Xetra (.DE), TSX (.V, .TO).
    """
    risultato = {
        "market_cap": None, "adtv_3m": None, "free_float_pct": None,
        "data_news": None, "delisting": False, "errore": None,
    }

    for tentativo in range(3):
        try:
            stock = yf.Ticker(ticker)
            info  = stock.info

            # Market Cap — più chiavi possibili
            mc = (info.get("marketCap")
                  or info.get("market_cap")
                  or info.get("enterpriseValue"))
            if mc:
                risultato["market_cap"] = int(mc)

            # ADTV 3M in USD
            adtv_shares = (info.get("averageDailyVolume3Month")
                           or info.get("averageVolume10days")
                           or info.get("averageVolume"))
            prezzo = (info.get("currentPrice")
                      or info.get("regularMarketPrice")
                      or info.get("previousClose")
                      or info.get("navPrice"))
            if adtv_shares and prezzo:
                risultato["adtv_3m"] = int(adtv_shares * prezzo)
            elif adtv_shares:
                storia = stock.history(period="3mo")
                if not storia.empty:
                    risultato["adtv_3m"] = int(
                        storia["Volume"].mean() * storia["Close"].mean()
                    )

            # Free Float
            float_s = info.get("floatShares")
            shares  = info.get("sharesOutstanding")
            if float_s and shares and shares > 0:
                risultato["free_float_pct"] = round(float_s / shares * 100, 2)

            # Delisting: MC e volume entrambi assenti
            vol_check = info.get("regularMarketVolume") or info.get("volume")
            if not mc and not vol_check:
                risultato["delisting"] = True

            # Data ultima news
            news = stock.news
            if news and isinstance(news, list) and len(news) > 0:
                prima = news[0]
                ts = (prima.get("providerPublishTime")
                      or prima.get("pubDate")
                      or (prima.get("content") or {}).get("providerPublishTime")
                      or (prima.get("content") or {}).get("pubDate"))
                if ts:
                    risultato["data_news"] = (
                        ts[:10] if isinstance(ts, str)
                        else datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    )

            break  # successo, esci dal loop

        except Exception as e:
            err_str = str(e)
            if tentativo < 2:
                time.sleep(2 ** tentativo)
            else:
                risultato["errore"] = err_str[:100]
                risultato["delisting"] = (
                    "delisted" in err_str.lower() or "no data" in err_str.lower()
                )

    return risultato


def get_brevetti_uspto(nome_azienda: str, max_tentativi: int = 3) -> dict:
    """
    Interroga USPTO PatentsView API per Granted e Pending.
    Retry con backoff + fallback prima parola.
    """
    risultato = {"granted": 0, "pending": 0, "errore": None}

    nome_pulito = (nome_azienda
                   .replace(" Inc.", "").replace(" Inc", "")
                   .replace(" Ltd.", "").replace(" Ltd", "")
                   .replace(" Corp.", "").replace(" Corp", "")
                   .replace(" S.A.", "").replace(" S.A", "")
                   .replace(" AG", "").replace(" plc", "")
                   .replace(" SE", "").replace(" NV", "")
                   .replace(" Holdings", "").replace(" Group", "")
                   .strip())

    def _fetch(url, params, campo, tentativi=max_tentativi):
        for i in range(tentativi):
            try:
                r = requests.get(url, params=params, timeout=15)
                if r.status_code == 200:
                    return r.json().get(campo, 0) or 0
                elif r.status_code == 429:
                    time.sleep(6 * (i + 1))
                elif r.status_code >= 500:
                    time.sleep(2 ** i)
                else:
                    return 0
            except requests.exceptions.Timeout:
                time.sleep(2 ** i)
            except Exception:
                time.sleep(2 ** i)
        return 0

    base_granted = "https://search.patentsview.org/api/v1/patent/"
    base_pending = "https://search.patentsview.org/api/v1/publication/"

    try:
        params = {
            "q": f'{{"assignee_organization": "{nome_pulito}"}}',
            "f": '["patent_id","assignee_organization"]',
            "o": '{"per_page": 1}',
        }
        count = _fetch(base_granted, params, "total_patent_count")
        # Fallback prima parola se 0
        if count == 0 and " " in nome_pulito:
            prima = nome_pulito.split()[0]
            params_fb = {
                "q": f'{{"assignee_organization": "{prima}"}}',
                "f": '["patent_id","assignee_organization"]',
                "o": '{"per_page": 1}',
            }
            count = _fetch(base_granted, params_fb, "total_patent_count")
        risultato["granted"] = count
    except Exception as e:
        risultato["errore"] = f"Granted: {e}"

    time.sleep(1.0)

    try:
        params = {
            "q": f'{{"assignee_organization": "{nome_pulito}"}}',
            "f": '["publication_id","assignee_organization"]',
            "o": '{"per_page": 1}',
        }
        risultato["pending"] = _fetch(base_pending, params, "total_publication_count")
    except Exception as e:
        risultato["errore"] = (risultato["errore"] or "") + f" | Pending: {e}"

    return risultato


def calcola_ges(tier: str, rev_pct: float, brevetti: int, pat_max: int) -> float:
    """GES_i = (α·Rev_i + β·Pat_i/Pat_max) · Ψ_i  [Rulebook Sez. 4-BIS]"""
    if tier not in GES_COEFFICIENTI:
        return 0.0
    c = GES_COEFFICIENTI[tier]
    pat_norm = min(brevetti / pat_max, 1.0) if pat_max > 0 else 0.0
    rev_norm = min(max(rev_pct, 0.0), 1.0)
    return round((c["alpha"] * rev_norm + c["beta"] * pat_norm) * c["psi"], 4)


def verifica_ammissione(mc, adtv, ff, ticker) -> str:
    if is_ashare(ticker):
        return "FAIL — A-Share cinese (Rulebook 6.1)"
    fail, warn = [], []
    if mc is None:          warn.append("MC N/D")
    elif mc < MIN_MARKET_CAP_USD: fail.append(f"MC {mc/1e6:.1f}M<10M")
    if adtv is None:        warn.append("ADTV N/D")
    elif adtv < MIN_ADTV_USD:     fail.append(f"ADTV ${adtv:,.0f}<$250K")
    if ff is None:          warn.append("Float N/D")
    elif ff < MIN_FREE_FLOAT_PCT: fail.append(f"Float {ff:.1f}%<15%")
    if fail:   return "FAIL — " + " | ".join(fail)
    if warn:   return "WARN — " + " | ".join(warn)
    return "PASS"


# ══════════════════════════════════════════════════════════════
# 4. MOTORE PRINCIPALE
# ══════════════════════════════════════════════════════════════

if st.button("🚀 AVVIA AGGIORNAMENTO", use_container_width=True, type="primary"):
    st.markdown("---")

    for nome_foglio in fogli_da_aggiornare:
        st.markdown(f"## 📡 FOGLIO: {nome_foglio}")

        try:
            ws = sh.worksheet(nome_foglio)
        except Exception:
            st.warning(f"Foglio '{nome_foglio}' non trovato — salto.")
            continue

        # ── Assicura colonne esistano nel foglio ──────────────
        if nome_foglio == FOGLIO_DB:
            col_map = assicura_colonne(ws, COLONNE_DB)
        else:
            col_map = assicura_colonne(ws, COLONNE_WL)

        records = ws.get_all_records()
        if not records:
            st.info("Foglio vuoto.")
            continue

        n = len(records)
        st.caption(f"{n} righe da processare")

        # ══════════════════════════════════════════════════
        # PASSAGGIO 1: raccoglie Yahoo + brevetti grezzo
        # ══════════════════════════════════════════════════
        st.markdown("#### Passaggio 1/2 — Raccolta dati (Yahoo Finance + USPTO)")
        progress = st.progress(0)
        status_box = st.empty()

        raccolta = []  # lista di dict con tutti i dati grezzi

        for i, riga in enumerate(records):
            ticker  = str(riga.get("Ticker", "")).strip()
            azienda = str(riga.get("Azienda", "")).strip()
            tier    = str(riga.get("Tier", "")).strip()
            riga_num = i + 2

            progress.progress((i + 1) / n,
                text=f"[{i+1}/{n}] {ticker} — {azienda[:25]}")
            status_box.caption(f"⏳ Elaborando `{ticker}`…")

            entry = {
                "ticker": ticker, "azienda": azienda, "tier": tier,
                "riga_num": riga_num,
                "rev_pct_manuale": riga.get("Rev_Grafene_Pct", ""),
            }

            if not ticker:
                raccolta.append(entry)
                continue

            # A-share
            if is_ashare(ticker):
                entry["ashare"] = True
                raccolta.append(entry)
                continue
            entry["ashare"] = False

            # Yahoo Finance
            dati_yf = get_dati_yahoo(ticker)
            time.sleep(0.6)
            entry.update({
                "market_cap":    dati_yf["market_cap"],
                "adtv_3m":       dati_yf["adtv_3m"],
                "free_float_pct": dati_yf["free_float_pct"],
                "data_news":     dati_yf["data_news"],
                "delisting":     dati_yf["delisting"],
                "yf_errore":     dati_yf["errore"],
            })

            # Brevetti USPTO (solo Database)
            if nome_foglio == FOGLIO_DB and azienda:
                # Legge brevetti esistenti come punto di partenza
                g_exist = safe_int(riga.get("Brevetti_Granted", 0))
                p_exist = safe_int(riga.get("Brevetti_Pending", 0))

                brev = get_brevetti_uspto(azienda)
                if not brev["errore"] and (brev["granted"] + brev["pending"]) > 0:
                    entry["brevetti_granted"] = brev["granted"]
                    entry["brevetti_pending"] = brev["pending"]
                    entry["brevetti_fonte"]   = "USPTO"
                else:
                    # Mantiene il valore esistente nel DB se USPTO non risponde
                    entry["brevetti_granted"] = g_exist
                    entry["brevetti_pending"] = p_exist
                    entry["brevetti_fonte"]   = "DB (USPTO N/D)"
            else:
                entry["brevetti_granted"] = 0
                entry["brevetti_pending"] = 0
                entry["brevetti_fonte"]   = "N/A"

            raccolta.append(entry)

        progress.empty()
        status_box.empty()
        st.success(f"✅ Passaggio 1 completato — {n} ticker analizzati")

        # ══════════════════════════════════════════════════
        # PASSAGGIO 2: calcola GES con Pat_max REALE
        # ══════════════════════════════════════════════════
        st.markdown("#### Passaggio 2/2 — Calcolo GES con Pat_max reale")

        # Pat_max = massimo brevetti totali nell'universo (post-aggiornamento)
        tutti_brevetti = [
            e.get("brevetti_granted", 0) + e.get("brevetti_pending", 0)
            for e in raccolta
        ]
        pat_max_reale = max(tutti_brevetti) if tutti_brevetti else 1
        pat_max_reale = max(pat_max_reale, 1)  # mai 0
        st.caption(f"Pat_max reale (post-USPTO): **{pat_max_reale:,}** brevetti")

        aggiornamenti_batch = []
        righe_preview = []

        for entry in raccolta:
            ticker   = entry.get("ticker", "")
            azienda  = entry.get("azienda", "")
            tier     = entry.get("tier", "")
            riga_num = entry.get("riga_num")

            if not ticker:
                continue

            # A-share
            if entry.get("ashare"):
                aggiornamenti_batch.append({
                    "riga": riga_num,
                    "Flag_Ammissione": "FAIL — A-Share cinese (Rulebook 6.1)",
                    "Flag_Delisting":  "N/A",
                })
                righe_preview.append({
                    "Ticker": ticker, "Tier": tier,
                    "Flag": "FAIL A-Share",
                    "MC": "—", "GES": "—", "Brevetti": "—",
                })
                continue

            mc   = entry.get("market_cap")
            adtv = entry.get("adtv_3m")
            ff   = entry.get("free_float_pct")

            # Calcola GES per Database
            ges_score = 0.0
            fonte_rev = "—"
            if nome_foglio == FOGLIO_DB and tier in GES_COEFFICIENTI:
                rev_raw = entry.get("rev_pct_manuale", "")
                if rev_raw not in (None, "", "N/D", "0", 0):
                    try:
                        rev_pct = min(max(
                            float(str(rev_raw).replace("%", "")) / 100, 0.0), 1.0
                        )
                        fonte_rev = f"manuale ({rev_pct*100:.1f}%)"
                    except ValueError:
                        rev_pct = STIMA_REV_PER_TIER.get(tier, 0.05)
                        fonte_rev = f"stima {tier}"
                else:
                    rev_pct = STIMA_REV_PER_TIER.get(tier, 0.05)
                    fonte_rev = f"stima {tier}"

                brevetti_tot = (entry.get("brevetti_granted", 0)
                                + entry.get("brevetti_pending", 0))
                ges_score = calcola_ges(tier, rev_pct, brevetti_tot, pat_max_reale)

            # Flag ammissione
            flag_amm = verifica_ammissione(mc, adtv, ff, ticker)
            flag_del = "ALERT" if entry.get("delisting") else "OK"

            # Costruisce update
            upd = {
                "riga":            riga_num,
                "Data_Ultima_News": entry.get("data_news"),
                "Market_Cap_USD":  mc,
                "ADTV_3M_USD":     adtv,
                "Free_Float_Pct":  ff,
                "Flag_Ammissione": flag_amm,
                "Flag_Delisting":  flag_del,
            }
            if nome_foglio == FOGLIO_DB:
                upd["Brevetti_Granted"] = entry.get("brevetti_granted", 0)
                upd["Brevetti_Pending"] = entry.get("brevetti_pending", 0)
                upd["GES_Score"]        = ges_score

            # Applica filtro colonne selezionate
            sel = colonne_da_aggiornare
            if "Solo MC/ADTV/Float" in sel and "Tutte" not in sel:
                upd = {k: v for k, v in upd.items()
                       if k in ("riga", "Market_Cap_USD", "ADTV_3M_USD",
                                "Free_Float_Pct", "Flag_Ammissione", "Flag_Delisting")}
            elif "Solo GES/Brevetti" in sel and "Tutte" not in sel:
                upd = {k: v for k, v in upd.items()
                       if k in ("riga", "Brevetti_Granted", "Brevetti_Pending", "GES_Score")}
            elif "Solo News/Flags" in sel and "Tutte" not in sel:
                upd = {k: v for k, v in upd.items()
                       if k in ("riga", "Data_Ultima_News", "Flag_Ammissione", "Flag_Delisting")}

            aggiornamenti_batch.append(upd)

            # Preview
            righe_preview.append({
                "Ticker":    ticker,
                "Azienda":   azienda[:20],
                "Tier":      tier,
                "MC":        f"${mc/1e9:.1f}B" if mc and mc >= 1e9 else (f"${mc/1e6:.0f}M" if mc else "N/D"),
                "ADTV":      f"${adtv/1e6:.1f}M" if adtv and adtv >= 1e6 else (f"${adtv:,.0f}" if adtv else "N/D"),
                "Float%":    f"{ff:.1f}%" if ff else "N/D",
                "GES":       f"{ges_score:.4f}" if nome_foglio == FOGLIO_DB else "—",
                "Brevetti":  f"{entry.get('brevetti_granted',0)}G / {entry.get('brevetti_pending',0)}P ({entry.get('brevetti_fonte','—')})" if nome_foglio == FOGLIO_DB else "—",
                "Rev fonte": fonte_rev if nome_foglio == FOGLIO_DB else "—",
                "Flag":      flag_amm,
                "News":      entry.get("data_news", "N/D") or "N/D",
            })

        # ── Tabella preview ───────────────────────────────────
        st.markdown("#### Anteprima valori calcolati")
        df_preview = pd.DataFrame(righe_preview)
        st.dataframe(df_preview, use_container_width=True, hide_index=True,
                     height=min(600, (len(df_preview) + 1) * 38))

        # ── Scrittura su Sheets (solo se non DRY RUN) ─────────
        if dry_run:
            st.warning("🟡 DRY RUN — Nessuna modifica scritta su Google Sheets.")
            continue

        st.markdown("#### Scrittura su Google Sheets…")
        write_progress = st.progress(0)

        batch_data = []
        for upd in aggiornamenti_batch:
            riga_n = upd["riga"]
            for campo, valore in upd.items():
                if campo == "riga" or valore is None:
                    continue
                idx = col_map.get(campo)
                if idx is None:
                    continue
                col_letter = idx_to_col_letter(idx)
                batch_data.append({
                    "range":  f"{col_letter}{riga_n}",
                    "values": [[str(valore)]]
                })

        if batch_data:
            try:
                ws.batch_update(batch_data, value_input_option="USER_ENTERED")
                write_progress.progress(1.0)
                st.success(f"✅ Foglio '{nome_foglio}' aggiornato — {len(batch_data)} celle scritte.")
            except Exception as e:
                st.warning(f"Batch update fallito ({e}) — scrivo cella per cella…")
                for j, item in enumerate(batch_data):
                    try:
                        ws.update(item["range"], item["values"],
                                  value_input_option="USER_ENTERED")
                        time.sleep(1.2)
                        write_progress.progress((j + 1) / len(batch_data))
                    except Exception as e2:
                        st.warning(f"Errore cella {item['range']}: {e2}")
        else:
            st.info("Nessuna cella da aggiornare.")

    # ── Riepilogo finale ──────────────────────────────────────
    st.markdown("---")
    st.markdown("### ✅ AGGIORNAMENTO COMPLETATO")
    col_r1, col_r2, col_r3 = st.columns(3)
    col_r1.metric("Data esecuzione", datetime.now().strftime("%d/%m/%Y %H:%M"))
    col_r2.metric("Modalità", "DRY RUN" if dry_run else "LIVE")
    col_r3.metric("Fogli processati", str(len(fogli_da_aggiornare)))

    if not dry_run:
        st.info(
            "💡 Prossimi passi:\n"
            "1. Ricarica il GGIV Terminal — i pesi RawScore saranno ora corretti\n"
            "2. Verifica che il Golden Shield sia ≥30% nella scheda DATABASE & DSRM\n"
            "3. Se Rev_Grafene_Pct è ancora vuota per alcuni titoli, inseriscila manualmente\n"
            "4. Prossimo run consigliato: lunedì mattina o prima del ribilanciamento trimestrale"
        )
