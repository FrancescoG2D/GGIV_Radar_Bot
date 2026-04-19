"""
GGIV UPDATE TOOL — v2.0
========================
Potenziamento completo del news bot originale.

COSA FA:
  - Aggiorna Data_Ultima_News (come prima)
  - Aggiunge Market Cap, ADTV 3 mesi, Free Float automaticamente
  - Verifica filtri ammissione Rulebook sezione 2 (Market Cap >10M, ADTV >250K, Float >15%)
  - Blocca i ticker A-share cinesi (.SS / .SZ) con flag automatico
  - Rileva potenziali delisting (NaN persistenti da Yahoo Finance)
  - Conta brevetti USPTO (Granted + Pending) via API pubblica gratuita
  - Calcola il GES reale con coefficienti α/β per Tier (Rulebook v1.3)
  - Se inserisci solo Ticker + Tier, compila tutto il resto automaticamente

REQUISITI:
  pip install yfinance gspread requests

STRUTTURA GOOGLE SHEET:
  Foglio "Database" — colonne richieste:
    Ticker, Azienda, Tier, Peso_Base, Data_Ultima_News,
    Market_Cap_USD, ADTV_3M_USD, Free_Float_Pct,
    Brevetti_Granted, Brevetti_Pending, GES_Score,
    Flag_Ammissione, Flag_Delisting

  Foglio "Watchlist" — colonne richieste:
    Ticker, Azienda, Data_Ultima_News,
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
import json
from datetime import datetime, timedelta

st.title("⬡ GGIV UPDATE TOOL v2.0")
st.caption("Vault Algorithm — Modulo di Aggiornamento Google Sheets")
st.markdown("---")

# ══════════════════════════════════════════════════════════════
# CONFIGURAZIONE
# ══════════════════════════════════════════════════════════════

NOME_FILE_GOOGLE = "GGIV_Database"
FOGLI            = ["Database", "Watchlist"]

# Soglie filtri ammissione (Rulebook sezione 2)
MIN_MARKET_CAP_USD = 10_000_000    # 10 milioni USD
MIN_ADTV_USD       = 250_000       # 250.000 USD volume medio 3 mesi
MIN_FREE_FLOAT_PCT = 15.0          # 15% flottante minimo

# Coefficienti GES per Tier (Rulebook v1.3 — Sezione 4-BIS)
# Tier 1: brevetti pesano di più (aziende pre-revenue)
# Tier 2: mix bilanciato
# Tier 3: revenue pesa di più (aziende mature)
GES_COEFFICIENTI = {
    "Tier 1": {"alpha": 0.30, "beta": 0.70},
    "Tier 2": {"alpha": 0.55, "beta": 0.45},
    "Tier 3": {"alpha": 0.70, "beta": 0.30},
}

# Moltiplicatori Tier (Rulebook sezione 3)
TIER_PSI = {"Tier 1": 1.5, "Tier 2": 1.0, "Tier 3": 0.5}

# Ticker A-share cinesi da bloccare (Rulebook sezione 6.1)
SUFFIX_ASHARE = [".SS", ".SZ"]  # Shanghai e Shenzhen

# ══════════════════════════════════════════════════════════════
# 1. CONNESSIONE GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════

try:
    # Legge le credenziali dai Secrets di Streamlit Cloud
    # Configura nei Secrets: [gcp_service_account] con il contenuto del tuo JSON
    creds_dict = dict(st.secrets["gcp_service_account"])
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(NOME_FILE_GOOGLE)
    st.success(f"✅ Connesso a Google Sheets: {NOME_FILE_GOOGLE}")
except Exception as e:
    st.error(f"❌ ERRORE CONNESSIONE: {e}")
    st.stop()

# ══════════════════════════════════════════════════════════════
# 2. FUNZIONI DI SUPPORTO
# ══════════════════════════════════════════════════════════════

def is_ashare(ticker: str) -> bool:
    """Blocca i ticker A-share cinesi (Shanghai .SS / Shenzhen .SZ)."""
    t = ticker.upper()
    return any(t.endswith(s) for s in SUFFIX_ASHARE)


def get_dati_yahoo(ticker: str) -> dict:
    """
    Scarica da Yahoo Finance:
    - Prezzo e volume per ADTV 3 mesi
    - Market Cap
    - Free Float (floatShares / sharesOutstanding)
    - Data ultima news
    Ritorna un dizionario con tutti i valori o None se il ticker non esiste.
    """
    risultato = {
        "market_cap":    None,
        "adtv_3m":       None,
        "free_float_pct": None,
        "data_news":     None,
        "delisting":     False,
        "errore":        None,
    }

    try:
        stock = yf.Ticker(ticker)
        info  = stock.info

        # ── Market Cap ────────────────────────────────────────
        mc = info.get("marketCap") or info.get("market_cap")
        risultato["market_cap"] = int(mc) if mc else None

        # ── ADTV 3 mesi ──────────────────────────────────────
        # Yahoo espone averageVolume10days e averageDailyVolume3Month
        # Usiamo il prezzo corrente * volume medio per avere il valore in USD
        adtv_shares = (info.get("averageDailyVolume3Month")
                       or info.get("averageVolume10days")
                       or info.get("averageVolume"))
        prezzo      = (info.get("currentPrice")
                       or info.get("regularMarketPrice")
                       or info.get("previousClose"))

        if adtv_shares and prezzo:
            risultato["adtv_3m"] = int(adtv_shares * prezzo)
        elif adtv_shares:
            # Fallback: scarica storia recente e calcola
            storia = stock.history(period="3mo")
            if not storia.empty:
                vol_medio  = storia["Volume"].mean()
                prezzo_med = storia["Close"].mean()
                risultato["adtv_3m"] = int(vol_medio * prezzo_med)

        # ── Free Float ────────────────────────────────────────
        float_shares = info.get("floatShares")
        shares_out   = info.get("sharesOutstanding")
        if float_shares and shares_out and shares_out > 0:
            risultato["free_float_pct"] = round((float_shares / shares_out) * 100, 2)

        # ── Delisting detector ────────────────────────────────
        # Se marketCap è None E volume è None → probabile delisting
        vol_check = info.get("regularMarketVolume") or info.get("volume")
        if not mc and not vol_check:
            risultato["delisting"] = True

        # ── Data ultima news ──────────────────────────────────
        news = stock.news
        if news and isinstance(news, list) and len(news) > 0:
            prima = news[0]
            ts = (prima.get("providerPublishTime")
                  or prima.get("pubDate")
                  or (prima.get("content", {}) or {}).get("providerPublishTime")
                  or (prima.get("content", {}) or {}).get("pubDate"))
            if ts:
                if isinstance(ts, str):
                    risultato["data_news"] = ts[:10]
                else:
                    risultato["data_news"] = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

    except Exception as e:
        risultato["errore"] = str(e)
        # Dopo 3 errori consecutivi su stesso ticker → flag delisting
        risultato["delisting"] = "delisted" in str(e).lower() or "no data" in str(e).lower()

    return risultato


def get_brevetti_uspto(nome_azienda: str) -> dict:
    """
    Interroga l'API pubblica USPTO (PatentsView) per contare:
    - Brevetti Granted (concessi)
    - Brevetti Pending (domande attive)

    API gratuita, nessuna chiave richiesta.
    Documentazione: https://patentsview.org/apis/api-endpoints/patents
    """
    risultato = {"granted": 0, "pending": 0, "errore": None}

    # Pulisce il nome per la ricerca (rimuove Inc., Ltd., Corp. ecc.)
    nome_pulito = (nome_azienda
                   .replace(" Inc.", "").replace(" Inc", "")
                   .replace(" Ltd.", "").replace(" Ltd", "")
                   .replace(" Corp.", "").replace(" Corp", "")
                   .replace(" S.A.", "").replace(" AG", "")
                   .replace(" plc", "").strip())

    # ── Brevetti Granted ─────────────────────────────────────
    try:
        url_granted = "https://search.patentsview.org/api/v1/patent/"
        params = {
            "q": f'{{"assignee_organization": "{nome_pulito}"}}',
            "f": '["patent_id","patent_date","assignee_organization"]',
            "o": '{"per_page": 1}',
        }
        r = requests.get(url_granted, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            risultato["granted"] = data.get("total_patent_count", 0) or 0
    except Exception as e:
        risultato["errore"] = f"Granted: {e}"

    time.sleep(0.5)  # Rispetta i rate limit USPTO

    # ── Brevetti Pending (Publication API) ───────────────────
    try:
        url_pending = "https://search.patentsview.org/api/v1/publication/"
        params = {
            "q": f'{{"assignee_organization": "{nome_pulito}"}}',
            "f": '["publication_id","assignee_organization"]',
            "o": '{"per_page": 1}',
        }
        r = requests.get(url_pending, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            risultato["pending"] = data.get("total_publication_count", 0) or 0
    except Exception as e:
        if risultato["errore"]:
            risultato["errore"] += f" | Pending: {e}"
        else:
            risultato["errore"] = f"Pending: {e}"

    return risultato


def calcola_ges(tier: str, rev_pct: float, brevetti: int,
                pat_max: int) -> float:
    """
    Calcola il GES (Graphene Exposure Score) secondo Rulebook v1.3 — Sezione 4-BIS.

    Parametri:
        tier      : "Tier 1" / "Tier 2" / "Tier 3"
        rev_pct   : percentuale fatturato da grafene [0.0 - 1.0]
                    (letta da colonna Rev_Grafene_Pct nel CSV, se presente)
        brevetti  : totale brevetti (granted + pending) dell'azienda
        pat_max   : brevetti massimi nell'universo investibile (normalizzazione)

    Formula:
        GES_i = (α · Rev_i + β · Pat_i/Pat_max) · Psi_i
    """
    if tier not in GES_COEFFICIENTI:
        return 0.0

    coeff = GES_COEFFICIENTI[tier]
    alpha = coeff["alpha"]
    beta  = coeff["beta"]
    psi   = TIER_PSI.get(tier, 1.0)

    # Normalizzazione brevetti
    pat_norm = (brevetti / pat_max) if pat_max > 0 else 0.0
    pat_norm = min(pat_norm, 1.0)  # Cap a 1.0

    # Rev_pct già in [0,1]
    rev_norm = min(max(rev_pct, 0.0), 1.0)

    ges = (alpha * rev_norm + beta * pat_norm) * psi
    return round(ges, 4)


def verifica_ammissione(market_cap, adtv, free_float_pct, ticker) -> str:
    """
    Verifica i filtri di ammissione del Rulebook sezione 2.
    Ritorna "PASS", "FAIL" o "WARN" con motivazione.
    """
    if is_ashare(ticker):
        return "FAIL — A-Share cinese (Rulebook 6.1)"

    motivi_fail = []
    motivi_warn = []

    if market_cap is None:
        motivi_warn.append("Market Cap N/D")
    elif market_cap < MIN_MARKET_CAP_USD:
        motivi_fail.append(f"Market Cap {market_cap/1e6:.1f}M < 10M")

    if adtv is None:
        motivi_warn.append("ADTV N/D")
    elif adtv < MIN_ADTV_USD:
        motivi_fail.append(f"ADTV ${adtv:,.0f} < $250K")

    if free_float_pct is None:
        motivi_warn.append("Free Float N/D")
    elif free_float_pct < MIN_FREE_FLOAT_PCT:
        motivi_fail.append(f"Float {free_float_pct:.1f}% < 15%")

    if motivi_fail:
        return "FAIL — " + " | ".join(motivi_fail)
    elif motivi_warn:
        return "WARN — " + " | ".join(motivi_warn)
    return "PASS"


# ══════════════════════════════════════════════════════════════
# 3. MOTORE PRINCIPALE — SCANSIONE FOGLI
# ══════════════════════════════════════════════════════════════

if st.button("🚀 AVVIA AGGIORNAMENTO", use_container_width=True, type="primary"):
 st.markdown("---")
 for nome_foglio in FOGLI:
    st.markdown(f"### 📡 SCANSIONE FOGLIO: {nome_foglio}")

    try:
        ws = sh.worksheet(nome_foglio)
    except Exception:
        st.warning(f"⚠️ Foglio '{nome_foglio}' non trovato. Salto.")
        continue

    records    = ws.get_all_records()
    intestazioni = ws.row_values(1)

    if not records:
        st.info("Foglio vuoto.")
        continue

    # ── Mappa indici colonne ──────────────────────────────────
    def col_idx(nome):
        """Ritorna l'indice (1-based) di una colonna, o None se non esiste."""
        try:
            return intestazioni.index(nome) + 1
        except ValueError:
            return None

    # Colonne obbligatorie per questo tool
    COLONNE_RICHIESTE = {
        "Data_Ultima_News": col_idx("Data_Ultima_News"),
        "Market_Cap_USD":   col_idx("Market_Cap_USD"),
        "ADTV_3M_USD":      col_idx("ADTV_3M_USD"),
        "Free_Float_Pct":   col_idx("Free_Float_Pct"),
        "Flag_Ammissione":  col_idx("Flag_Ammissione"),
        "Flag_Delisting":   col_idx("Flag_Delisting"),
    }

    # Colonne solo per Database (non Watchlist)
    if nome_foglio == "Database":
        COLONNE_RICHIESTE.update({
            "Brevetti_Granted": col_idx("Brevetti_Granted"),
            "Brevetti_Pending": col_idx("Brevetti_Pending"),
            "GES_Score":        col_idx("GES_Score"),
        })

    # Avvisa se mancano colonne
    mancanti = [n for n, i in COLONNE_RICHIESTE.items() if i is None]
    if mancanti:
        st.warning(f"⚠️ COLONNE MANCANTI nel foglio '{nome_foglio}': {', '.join(mancanti)}. Il tool scriverà solo nelle colonne esistenti.")

    # ── Calcola Pat_max per normalizzazione GES ───────────────
    # (solo per Database)
    pat_max_globale = 1  # default
    if nome_foglio == "Database":
        pat_totali = []
        for riga in records:
            g = int(riga.get("Brevetti_Granted", 0) or 0)
            p = int(riga.get("Brevetti_Pending", 0) or 0)
            pat_totali.append(g + p)
        pat_max_globale = max(pat_totali) if pat_totali else 1

    # ── Scansione riga per riga ───────────────────────────────
    aggiornamenti_batch = []  # Raccoglie tutti gli update per batch write

    for i, riga in enumerate(records):
        ticker    = str(riga.get("Ticker", "")).strip()
        azienda   = str(riga.get("Azienda", "")).strip()
        tier      = str(riga.get("Tier", "")).strip()
        riga_num  = i + 2  # +1 header, +1 per index 0-based

        if not ticker:
            continue

        st.write(f"**[{riga_num-1}/{len(records)}]** `{ticker}` — {azienda}")

        # ── Blocco A-share ────────────────────────────────────
        if is_ashare(ticker):
            st.error(f"🚫 A-SHARE BLOCCATO — {ticker} non ammesso (Rulebook 6.1)")
            aggiornamenti_batch.append({
                "riga": riga_num,
                "Flag_Ammissione": "FAIL — A-Share cinese (Rulebook 6.1)",
                "Flag_Delisting": "N/A",
            })
            continue

        # ── Yahoo Finance ─────────────────────────────────────

        dati_yf = get_dati_yahoo(ticker)
        time.sleep(0.8)  # Rate limit rispettoso

        if dati_yf["errore"]:
            st.caption(f"⚠️ Yahoo: {dati_yf['errore'][:80]}")

        # Formatta Market Cap per leggibilità
        mc = dati_yf["market_cap"]
        mc_str = f"{mc:,}" if mc else "N/D"
        adtv   = dati_yf["adtv_3m"]
        adtv_str = f"{adtv:,}" if adtv else "N/D"
        ff     = dati_yf["free_float_pct"]
        ff_str = f"{ff:.1f}%" if ff else "N/D"

        st.caption(f"Market Cap: ${mc_str} | ADTV: ${adtv_str} | Float: {ff_str}")

        # ── Brevetti USPTO (solo Database) ───────────────────
        brevetti_granted = int(riga.get("Brevetti_Granted", 0) or 0)
        brevetti_pending = int(riga.get("Brevetti_Pending", 0) or 0)

        if nome_foglio == "Database" and azienda:

            brev = get_brevetti_uspto(azienda)
            if not brev["errore"]:
                brevetti_granted = brev["granted"]
                brevetti_pending = brev["pending"]
                st.caption(f"Brevetti Granted: {brevetti_granted} | Pending: {brevetti_pending}")

            # Ricalcola Pat_max con dati aggiornati
            tot_brev = brevetti_granted + brevetti_pending
            if tot_brev > pat_max_globale:
                pat_max_globale = tot_brev

        # ── Calcolo GES (solo Database) ───────────────────────
        ges_score = float(riga.get("GES_Score", 0) or 0)
        if nome_foglio == "Database" and tier in GES_COEFFICIENTI:
            # Legge Rev_Grafene_Pct se presente nel foglio
            rev_pct_raw = riga.get("Rev_Grafene_Pct", None)
            fonte_rev   = "manuale"

            if rev_pct_raw not in (None, "", "N/D"):
                # Valore inserito manualmente — massima priorità
                try:
                    rev_pct = float(str(rev_pct_raw).replace("%", "")) / 100
                    rev_pct = min(max(rev_pct, 0.0), 1.0)  # Clamp [0,1]
                except ValueError:
                    rev_pct = None

            else:
                rev_pct = None

            # ── Stima automatica per Tier se cella vuota ─────
            # Scala basata sulla realtà del settore grafene:
            # Tier 1: aziende pre-revenue o early stage → stima bassa/nulla
            # Tier 2: supply chain con ricavi da materiali → stima media
            # Tier 3: mega-cap con integrazione marginale → stima molto bassa
            if rev_pct is None:
                STIMA_PER_TIER = {
                    "Tier 1": 0.05,   # 5% — spesso pre-revenue, conta i brevetti
                    "Tier 2": 0.30,   # 30% — supply chain con ricavi da materiali
                    "Tier 3": 0.02,   # 2%  — mega-cap, integrazione marginale
                }
                rev_pct   = STIMA_PER_TIER.get(tier, 0.10)
                fonte_rev = f"stima automatica per {tier}"

            ges_score = calcola_ges(
                tier=tier,
                rev_pct=rev_pct,
                brevetti=brevetti_granted + brevetti_pending,
                pat_max=pat_max_globale,
            )
            st.caption(f"GES: {ges_score:.4f} | Rev: {rev_pct*100:.1f}% ({fonte_rev})")

        # ── Flag Ammissione ───────────────────────────────────
        flag_amm = verifica_ammissione(mc, adtv, ff, ticker)
        flag_del = "ALERT" if dati_yf["delisting"] else "OK"

        if flag_amm == "PASS":
            st.caption(f"✅ {flag_amm}")
        elif "WARN" in flag_amm:
            st.caption(f"⚠️ {flag_amm}")
        else:
            st.error(f"❌ {flag_amm}")
        if flag_del == "ALERT":
            st.error(f"🚨 DELISTING ALERT: {ticker}")

        # ── Raccoglie aggiornamento ───────────────────────────
        update = {
            "riga":            riga_num,
            "Data_Ultima_News": dati_yf["data_news"],
            "Market_Cap_USD":  mc,
            "ADTV_3M_USD":     adtv,
            "Free_Float_Pct":  ff,
            "Flag_Ammissione": flag_amm,
            "Flag_Delisting":  flag_del,
        }
        if nome_foglio == "Database":
            update["Brevetti_Granted"] = brevetti_granted
            update["Brevetti_Pending"] = brevetti_pending
            update["GES_Score"]        = ges_score

        aggiornamenti_batch.append(update)

    # ── Scrittura batch su Google Sheets ─────────────────────
    # Raggruppa tutti gli update in una singola chiamata per
    # minimizzare le API call a Google Sheets
    st.info(f"📝 Scrittura su Google Sheets ({len(aggiornamenti_batch)} righe)...")

    for upd in aggiornamenti_batch:
        riga_n = upd["riga"]
        for campo, valore in upd.items():
            if campo == "riga" or valore is None:
                continue
            idx = COLONNE_RICHIESTE.get(campo)
            if idx is None:
                continue  # Colonna non presente nel foglio, salta
            try:
                ws.update_cell(riga_n, idx, valore)
                time.sleep(0.15)  # Evita rate limit Google Sheets API
            except Exception as e:
                st.warning(f"Errore scrittura {campo} riga {riga_n}: {e}")

    st.success(f"✅ Foglio '{nome_foglio}' aggiornato.")

# ══════════════════════════════════════════════════════════════
# 4. RIEPILOGO FINALE
# ══════════════════════════════════════════════════════════════
st.markdown("---")
st.success(f"✅ MISSIONE COMPLETATA — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
st.info("Prossimo run consigliato: domani mattina o prima di ogni ribilanciamento trimestrale.")
