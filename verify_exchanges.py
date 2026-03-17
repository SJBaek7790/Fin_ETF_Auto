import urllib.request
import ssl
import zipfile
import os
import json

def fetch_and_parse_master_files():
    base_url = "https://new.real.download.dws.co.kr/common/master/"
    exchanges = {
        "nas": "NASD",
        "nys": "NYSE",
        "ams": "AMEX"
    }
    
    ticker_map = {}
    ssl._create_default_https_context = ssl._create_unverified_context
    
    script_dir = "/tmp/verify_exchanges"
    os.makedirs(script_dir, exist_ok=True)
    os.chdir(script_dir)
    
    for val, excg_cd in exchanges.items():
        zip_file = f"{val}mst.cod.zip"
        txt_file = f"{val}mst.cod"
        url = f"{base_url}{zip_file}"
        
        print(f"Downloading {url}...")
        try:
            urllib.request.urlretrieve(url, zip_file)
            
            with zipfile.ZipFile(zip_file) as zf:
                zf.extractall()
                
            count = 0
            # Read line by line with cp949
            with open(txt_file, 'r', encoding='cp949', errors='ignore') as f:
                for line in f:
                    cols = line.strip().split('\t')
                    # the 5th column (index 4) should be the symbol based on pandas inspect
                    if len(cols) > 4:
                        symbol = cols[4].strip()
                        if symbol:
                            ticker_map[symbol] = excg_cd
                            count += 1
                            
            print(f"Parsed {count} symbols from {val} mapping to {excg_cd}")
        except Exception as e:
            print(f"Error processing {val}: {e}")
        
    return ticker_map

if __name__ == "__main__":
    ticker_map = fetch_and_parse_master_files()
    
    test_tickers = ["AAPL", "SPY", "QQQ", "DIA", "TSLA", "TQQQ", "SOXL"]
    print("\nTest Tickers Mapping:")
    for t in test_tickers:
        print(f"{t}: {ticker_map.get(t, 'Not Found')}")
        
    out_path = "/tmp/verify_exchanges/test_mapping.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(ticker_map, f, indent=4)
        
    print(f"\nSaved {len(ticker_map)} total mappings to {out_path}")
