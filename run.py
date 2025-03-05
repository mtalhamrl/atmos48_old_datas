import os
import json
import glob
import rasterio
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count

# Maksimum veritabanı bağlantı sayısını belirle
MAX_DB_CONNECTIONS = 100

class IndependentLegacyDataLoader:
    def __init__(self, db_connection_params):
        self.conn = psycopg2.connect(**db_connection_params)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

        self.data_procedures = {
            'wind_direction': 'process_wind_direction_data',
            'wind_speed': 'process_wind_speed_data',
            'wind_gust': 'process_wind_gust_data',
            'ice_mass': 'process_ice_mass_data',
            'ice_thickness': 'process_ice_thickness_data'
        }

        self.value_calculators = {
            'wind_direction': lambda x: np.mod((x * 0.1 + 180), 360).astype(np.float32),
            'wind_speed': lambda x: (x * 0.1).astype(np.float32),
            'wind_gust': lambda x: (x * 0.1).astype(np.float32),
            'ice_mass': lambda x: (x * 0.1).astype(np.float32),
            'ice_thickness': lambda x: (x * 0.1).astype(np.float32)
        }

    def load_legacy_data(self, data_type, pattern):
        try:
            full_pattern = os.path.join(os.getenv('GEOSERVER_DIR'), pattern)
            print(f"\n📊 {data_type.upper()} işlemi başlıyor...")
            print(f"   🔎 Aranılan dosya yolu: {full_pattern}")
            
            tiff_files = glob.glob(full_pattern)
            if not tiff_files:
                print(f"   ❌ {data_type.upper()} için dosya bulunamadı.")
                return

            print(f"   📌 Bulunan dosyalar: {tiff_files}")
            for tiff_file in sorted(tiff_files):
                self.process_tiff_file(data_type, tiff_file)

        except Exception as e:
            print(f"   ❌ {data_type.upper()} yüklenirken hata oluştu: {str(e)}")

    def process_tiff_file(self, data_type, tiff_path):
        print(f"\n   📥 {data_type.upper()} verisi işleniyor: {tiff_path}")

        try:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            conn.autocommit = False
            cur = conn.cursor()

            file_name = os.path.splitext(os.path.basename(tiff_path))[0]
            
            cur.execute("""
                SELECT tower_serial, mid_latitude, mid_longitude 
                FROM teias_towers 
                WHERE mid_latitude IS NOT NULL 
                AND mid_longitude IS NOT NULL
                ORDER BY tower_serial
            """)
            towers = cur.fetchall()

            valid_poles = [pole for pole in towers if -90 <= float(pole[1]) <= 90 and -180 <= float(pole[2]) <= 180]
            print(f"   📍 Toplam direk: {len(towers)}, Geçerli koordinat: {len(valid_poles)}")

            with rasterio.open(tiff_path) as src:
                base_time = datetime.strptime(str(src.descriptions[0]), "%Y-%m-%d_%H")
                band_count = min(src.count, 6)
                print(f"   🎛 Band sayısı: {band_count}, Başlangıç zamanı: {base_time}")

                rows, cols = zip(*[src.index(float(pole[2]), float(pole[1])) for pole in valid_poles])
                all_band_data = src.read()
                batch_size = 100000
                batch_data = []
                total_records = 0

                print("   🛠 Band verileri işleniyor...")
                for band_index in range(band_count):
                    band_num = band_index + 1
                    print(f"      ▶️ Band {band_num}/{band_count} işleniyor...")

                    band_description = str(src.descriptions[band_index]).strip()
                    calculated_values = self.value_calculators[data_type](all_band_data[band_index][rows, cols])

                    for idx, pole in enumerate(valid_poles):
                        batch_data.append({
                            'tower_serial': str(pole[0]).strip(),
                            'file_name': file_name,
                            'band': int(band_num),
                            'band_description': band_description,
                            'forecast_time': band_description,
                            'value': float(calculated_values[idx])
                        })
                        total_records += 1

                        if len(batch_data) >= batch_size:
                            print(f"         💾 Batch yazılıyor... (Toplam: {total_records:,} kayıt)")
                            procedure = self.data_procedures.get(data_type)
                            if procedure:
                                cur.execute(f"CALL {procedure}(%s, %s, %s);", (file_name, base_time, json.dumps(batch_data)))
                                conn.commit()
                            batch_data = []

                if batch_data:
                    print(f"         📌 Son batch yazılıyor... (Toplam: {total_records:,} kayıt)")
                    procedure = self.data_procedures.get(data_type)
                    if procedure:
                        cur.execute(f"CALL {procedure}(%s, %s, %s);", (file_name, base_time, json.dumps(batch_data)))
                        conn.commit()

                print(f"   ✅ {data_type.upper()} işlemi tamamlandı. Toplam {total_records:,} kayıt işlendi.")
                cur.close()
                conn.close()

        except Exception as e:
            print(f"   ❌ {data_type.upper()} işlemi başarısız: {str(e)}")

def process_data_type(data_config):
    load_dotenv()
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }
    loader = IndependentLegacyDataLoader(db_params)
    data_type, pattern = data_config
    loader.load_legacy_data(data_type, pattern)

def main():
    load_dotenv()
    data_configs = [
        ('wind_speed', os.getenv('WIND_SPEED_PATTERN')),
        ('wind_gust', os.getenv('WIND_GUST_PATTERN')),
        ('wind_direction', os.getenv('WIND_DIRECTION_PATTERN')),
        ('ice_mass', os.getenv('ICE_MASS_PATTERN')),
        ('ice_thickness', os.getenv('ICE_THICKNESS_PATTERN'))
    ]

    # Maksimum çekirdekleri ve bağlantı sınırını kullanarak işlem sayısını belirle
    num_processes = min(len(data_configs), cpu_count(), MAX_DB_CONNECTIONS)

    print("\n=== ✅ Veri İşlemleri Başlıyor ✅ ===")
    
    # Paralel işleme başlat
    with Pool(processes=num_processes) as pool:
        pool.map(process_data_type, data_configs)

    print("\n=== ✅ Tüm Veri İşlemleri Tamamlandı ✅ ===")

if __name__ == "__main__":
    main()
