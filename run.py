import os
import json
import glob
import rasterio
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import pprint

class IndependentLegacyDataLoader:
    def __init__(self, db_connection_params):
        self.conn = psycopg2.connect(**db_connection_params)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

        self.data_procedures = {
            'wind_direction': 'process_wind_direction_data',
            'wind_speed': 'process_wind_speed_data',
            'wind_gust' : 'process_wind_gust_data',
            'ice_mass' : 'process_ice_mass_data',
            'ice_thickness': 'process_ice_thickness_data'
        }

        self.value_calculators = {
            'wind_direction': lambda x: np.mod((x * 0.1 + 180), 360).astype(np.float32),
            'wind_speed' : lambda x: (x * 0.1).astype(np.float32),
            'wind_gust': lambda x: (x * 0.1).astype(np.float32),
            'ice_mass': lambda x: (x * 0.1).astype(np.float32),
            'ice_thickness': lambda x: (x * 0.1).astype(np.float32)
        }

    def get_valid_coordinates(self, poles):
        valid_poles = []
        valid_coords = []
        for pole in sorted(poles, key=lambda x: x[0]):
            try:
                lon, lat = float(pole[2]), float(pole[1])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    valid_poles.append(pole)
                    valid_coords.append((lon, lat))
            except (ValueError, TypeError):
                continue
        return valid_poles, valid_coords

    def load_legacy_data(self, data_type, pattern):
        try:
            full_pattern = os.path.join(os.getenv('GEOSERVER_DIR'), pattern)
            print(f"📂 Arama deseni: {full_pattern}")
            
            tiff_files = glob.glob(full_pattern)
            print(f"📌 Bulunan {data_type} dosyaları: {tiff_files}")

            if not tiff_files:
                print(f"⚠️ {data_type} için işlenecek TIFF dosyası bulunamadı.")
                return

            for tiff_file in sorted(tiff_files):
                self.process_tiff_file(data_type, tiff_file)

        except Exception as e:
            print(f"❌ {data_type} yüklenirken hata oluştu: {str(e)}")
            import traceback
            traceback.print_exc()

    def process_tiff_file(self, data_type, tiff_path):
        print(f"\n🔍 {data_type} için dosya işleniyor: {tiff_path}")

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
            valid_poles, valid_coords = self.get_valid_coordinates(towers)

            with rasterio.open(tiff_path) as src:
                print(f"🔎 Band descriptions (from raster): {src.descriptions}")

                if not src.descriptions or all(desc is None for desc in src.descriptions):
                    print("⚠️ Uyarı: TIFF dosyasındaki `descriptions` alanı boş veya `None`!")
                
                base_time = datetime.strptime(str(src.descriptions[0]), "%Y-%m-%d_%H")
                band_count = min(src.count, 6)

                try:
                    rows, cols = zip(*[src.index(lon, lat) for lon, lat in valid_coords])
                    rows = np.array(rows)
                    cols = np.array(cols)
                except Exception as e:
                    print(f"❌ Koordinat dönüşüm hatası: {str(e)}")
                    return False

                all_band_data = src.read()
                batch_data = []
                total_records = 0

                for band_index in range(band_count):
                    band_num = band_index + 1
                    band_description = str(src.descriptions[band_index]).strip()

                    print(f"✅ Band {band_num}: `{band_description}`")

                    band_data = all_band_data[band_index]
                    raw_values = band_data[rows, cols]
                    calculated_values = self.value_calculators[data_type](raw_values)

                    for idx, pole in enumerate(valid_poles):
                        batch_data.append({
                            'tower_serial': str(pole[0]).strip(),
                            'file_name': str(file_name).strip(),
                            'band': int(band_num),
                            'band_description': band_description,
                            'forecast_time': band_description,
                            'value': float(calculated_values[idx])
                        })
                        total_records += 1

                if batch_data:
                    print(f"📝 Son batch gönderiliyor... (Toplam: {total_records:,} kayıt)")
                    pprint.pprint(batch_data[:5])  # İlk 5 JSON kaydını yazdır

                    procedure = self.data_procedures.get(data_type)
                    if procedure:
                        cur.execute(f"""
                            CALL {procedure}(%s, %s, %s)
                        """, (file_name, base_time, json.dumps(batch_data)))
                        conn.commit()

                cur.close()
                conn.close()
                return True
        
        except Exception as e:
            print(f"❌ {data_type} TIFF işleme hatası: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

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
    # .env dosyasını yükle
    load_dotenv()

    # Veri tipleri ve pattern'ler
    data_configs = [
        ('wind_speed', os.getenv('WIND_SPEED_PATTERN')),
        ('wind_gust', os.getenv('WIND_GUST_PATTERN')),
        ('wind_direction', os.getenv('WIND_DIRECTION_PATTERN')),
        ('ice_mass', os.getenv('ICE_MASS_PATTERN')),
        ('ice_thickness', os.getenv('ICE_THICKNESS_PATTERN'))
    ]

    # Multiprocessing kaldırıldı, tek tek çalıştırılacak
    for config in data_configs:
        process_data_type(config)

if __name__ == "__main__":
    main()
