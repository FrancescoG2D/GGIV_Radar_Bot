import yfinance as yf
from datetime import datetime
import gspread

print("🤖 Avvio News Bot GGIV...")

# ==========================================
# 1. CONNESSIONE AL DATABASE GOOGLE SHEETS
# ==========================================
NOME_FILE_GOOGLE = "GGIV_Database"

try:
    gc = gspread.service_account(filename='chiave_google.json')
    sh = gc.open(NOME_FILE_GOOGLE)
    print(f"✅ Connesso con successo al file Excel: {NOME_FILE_GOOGLE}")
except Exception as e:
    print(f"❌ ERRORE DI CONNESSIONE: {e}")
    exit()

# ==========================================
# 2. I FOGLI DA LEGGERE
# ==========================================
fogli_da_scansionare = ["Database", "Watchlist"]

# ==========================================
# 3. IL MOTORE DEL RADAR BLINDATO E DETECTIVE
# ==========================================
for nome_foglio in fogli_da_scansionare:
    print(f"\n==========================================")
    print(f"📡 AVVIO RADAR SUL FOGLIO: {nome_foglio}")
    print(f"==========================================")
    
    try:
        worksheet = sh.worksheet(nome_foglio)
    except Exception:
        print(f"⚠️ Foglio '{nome_foglio}' non trovato. Salto al prossimo...")
        continue

    records = worksheet.get_all_records()
    if not records:
        continue
        
    intestazioni = worksheet.row_values(1)
    if "Data_Ultima_News" in intestazioni:
        colonna_data = intestazioni.index("Data_Ultima_News") + 1
    else:
        print(f"❌ Errore: Manca l'intestazione 'Data_Ultima_News' in {nome_foglio}.")
        continue

    for i, riga in enumerate(records):
        ticker = str(riga.get("Ticker", "")).strip() 
        riga_corrente = i + 2  
        
        if not ticker:
            continue
            
        print(f"  🔍 Cerco news per: {ticker}...")
        
        try:
            stock = yf.Ticker(ticker)
            news = stock.news
            
            if news and isinstance(news, list) and len(news) > 0:
                prima_notizia = news[0]
                
                # --- IL DETECTIVE DELLE DATE ---
                timestamp_news = None
                
                # Cerca in tutti i nuovi possibili nascondigli di Yahoo
                if 'providerPublishTime' in prima_notizia:
                    timestamp_news = prima_notizia['providerPublishTime']
                elif 'pubDate' in prima_notizia:
                    timestamp_news = prima_notizia['pubDate']
                elif 'content' in prima_notizia and 'providerPublishTime' in prima_notizia['content']:
                    timestamp_news = prima_notizia['content']['providerPublishTime']
                elif 'content' in prima_notizia and 'pubDate' in prima_notizia['content']:
                    timestamp_news = prima_notizia['content']['pubDate']
                    
                if timestamp_news:
                    # Se Yahoo manda una data in formato testo invece che in numeri
                    if isinstance(timestamp_news, str):
                        data_formattata = timestamp_news[:10] # Prende solo YYYY-MM-DD
                    else:
                        data_formattata = datetime.fromtimestamp(timestamp_news).strftime('%Y-%m-%d')
                        
                    worksheet.update_cell(riga_corrente, colonna_data, data_formattata)
                    print(f"    ✅ Aggiornato al: {data_formattata}")
                else:
                    # Se fallisce ancora, stampiamo le etichette per capire cosa si sono inventati!
                    etichette = list(prima_notizia.keys())
                    print(f"    ⚠️ Data non trovata. Yahoo ha inviato queste etichette: {etichette}")
            else:
                print(f"    ⚠️ Nessuna news trovata sui server per {ticker}.")
                
        except Exception as e:
            print(f"    ❌ Errore durante l'analisi di {ticker}: {e}")
            
    print(f"🏁 Scansione completata per il foglio: {nome_foglio}")

print("\n🎉 MISSIONE NOTTURNA COMPLETATA! Il database è aggiornato.")