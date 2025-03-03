import os
import json
import glob
import rasterio
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count

# Maksimum veritabanı bağlantı sayısı
MAX_DB_CONNECTIONS = 100

class IndependentLegacyDataLoader:
    def __init__(self, db_connection_params):
        """
        Bağımsız Legacy Data Loader
        
        :param db_connection_params: Veritabanı bağlantı parametreleri
        """
        # Veritabanı bağlantısı
        self.conn = psycopg2.connect(**db_connection_params)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

        # Veri işleme prosedürleri
        self.data_procedures = {
            'wind_speed': 'process_wind_speed_data',
            'wind_gust': 'process_wind_gust_data',
            'wind_direction': 'process_wind_direction_data',
            'ice_mass': 'process_ice_mass_data',
            'ice_thickness': 'process_ice_thickness_data'
        }

        # Değer hesaplama fonksiyonları
        self.value_calculators = {
            'wind_speed': lambda x: (x * 0.1).astype(np.float32),
            'wind_gust': lambda x: (x * 0.1).astype(np.float32),
            'wind_direction': lambda x: np.mod((x * 0.1 + 180), 360).astype(np.float32),
            'ice_mass': lambda x: (x * 0.1).astype(np.float32),
            'ice_thickness': lambda x: (x * 0.1).astype(np.float32)
        }

    def get_valid_coordinates(self, poles):
        """
        Geçerli koordinatları filtrele
        
        :param poles: Tüm kuleler
        :return: Geçerli kuleler ve koordinatları
        """
        valid_poles = []
        valid_coords = []
        invalid_count = 0
        
        for pole in sorted(poles, key=lambda x: x[0]):
            try:
                lon, lat = float(pole[2]), float(pole[1])
                
                # Koordinat geçerlilik kontrolü
                if (not np.isnan(lon) and not np.isnan(lat) and 
                    -90 <= lat <= 90 and -180 <= lon <= 180):
                    valid_poles.append(pole)
                    valid_coords.append((lon, lat))
                else:
                    invalid_count += 1
            except (ValueError, TypeError):
                invalid_count += 1
        
        print(f"Toplam kule sayısı: {len(poles)}")
        print(f"Geçersiz koordinat sayısı: {invalid_count}")
        print(f"Geçerli koordinat sayısı: {len(valid_coords)}")
        
        return valid_poles, valid_coords

    def process_tiff_file(self, data_type, tiff_path):
        """
        TIFF dosyasını işle ve veritabanına kaydet
        
        :param data_type: Veri tipi
        :param tiff_path: TIFF dosyasının tam yolu
        """
        print(f"\n{data_type} için dosya işleniyor: {tiff_path}")
        
        try:
            # Veritabanı bağlantısı oluştur
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            conn.autocommit = False
            cur = conn.cursor()

            # Dosya adını basitleştir
            file_name = os.path.splitext(os.path.basename(tiff_path))[0]
            
            # Tüm direk koordinatlarını al
            cur.execute("""
                SELECT tower_serial, mid_latitude, mid_longitude 
                FROM teias_towers 
                WHERE mid_latitude IS NOT NULL 
                AND mid_longitude IS NOT NULL
                ORDER BY tower_serial
            """)
            towers = cur.fetchall()

            # Geçerli koordinatları al
            valid_poles, valid_coords = self.get_valid_coordinates(towers)

            # Raster dosyasını aç
            with rasterio.open(tiff_path) as src:
                # Temel zaman bilgisini al
                base_time = datetime.strptime(src.descriptions[0], "%Y-%m-%d_%H")
                
                # Band sayısını belirle (maksimum 6 band)
                band_count = min(src.count, 48)
                
                # Koordinat indexlerini hesapla
                try:
                    rows, cols = zip(*[src.index(lon, lat) for lon, lat in valid_coords])
                    rows = np.array(rows)
                    cols = np.array(cols)
                except Exception as e:
                    print(f"Koordinat dönüşüm hatası: {str(e)}")
                    return False

                # Tüm band verilerini oku
                all_band_data = src.read()
                
                # Toplu veri için liste
                batch_data = []
                total_records = 0
                
                # Her band için işlem
                for band_index in range(band_count):
                    band_num = band_index + 1
                    band_data = all_band_data[band_index]
                    band_time = src.descriptions[band_index]
                    
                    # Bandın ham verilerini al
                    raw_values = band_data[rows, cols]
                    
                    # Değerleri hesapla
                    calculated_values = self.value_calculators[data_type](raw_values)

                    # Her kule için değer ekle
                    for idx, pole in enumerate(valid_poles):
                        batch_data.append({
                            'tower_serial': pole[0],
                            'file_name': file_name,
                            'band': band_num,
                            'band_time': band_time,
                            'value': float(calculated_values[idx])
                        })
                        total_records += 1

                        # Toplu veri boyutunu kontrol et
                        if len(batch_data) >= 100000:
                            # Veritabanına kaydet
                            procedure = self.data_procedures.get(data_type)
                            if procedure:
                                cur.execute(f"""
                                    CALL {procedure}(%s, %s, %s)
                                """, (file_name, base_time, json.dumps(batch_data)))
                                conn.commit()
                                print(f"Batch yazıldı: {len(batch_data)} kayıt")
                                batch_data = []

                # Kalan verileri kaydet
                if batch_data:
                    procedure = self.data_procedures.get(data_type)
                    if procedure:
                        cur.execute(f"""
                            CALL {procedure}(%s, %s, %s)
                        """, (file_name, base_time, json.dumps(batch_data)))
                        conn.commit()
                        print(f"{data_type} için {file_name} işlendi. Toplam {total_records} kayıt")
                    else:
                        print(f"İşlem prosedürü bulunamadı: {data_type}")

                # Bağlantıları kapat
                cur.close()
                conn.close()

                return True
        
        except Exception as e:
            print(f"{data_type} TIFF işleme hatası: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def load_legacy_data(self, data_type, pattern):
        """
        Belirli bir veri tipi için tüm TIFF dosyalarını yükle
        
        :param data_type: Veri tipi
        :param pattern: TIFF dosyaları için arama deseni
        """
        try:
            # Dosyaları bul
            full_pattern = os.path.join(os.getenv('GEOSERVER_DIR'), pattern)
            print(f"Arama deseni: {full_pattern}")
            
            tiff_files = glob.glob(full_pattern)
            print(f"Bulunan {data_type} dosyaları: {tiff_files}")
            
            # Sırala ve işle
            if not tiff_files:
                print(f"{data_type} için dosya bulunamadı.")
                return
            
            # Sıralanmış dosyaları işle
            for tiff_file in sorted(tiff_files):
                self.process_tiff_file(data_type, tiff_file)
        
        except Exception as e:
            print(f"{data_type} yüklemesinde hata: {str(e)}")
            import traceback
            traceback.print_exc()

def process_data_type(data_config):
    """
    Tek bir veri tipi için veri işleme fonksiyonu
    
    :param data_config: (data_type, pattern) tuple'ı
    """
    load_dotenv()

    # Veritabanı bağlantı parametreleri
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    # Legacy data yükleyici
    loader = IndependentLegacyDataLoader(db_params)

    # Veri tipini yükle
    data_type, pattern = data_config
    print(f"\n{data_type.upper()} için eski veriler yükleniyor...")
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

    # İşlemci sayısını al (kullanılabilir çekirdek sayısı ve maks bağlantı sayısı arasındaki minimum)
    num_processes = min(len(data_configs), cpu_count(), MAX_DB_CONNECTIONS)

    # Çoklu işlem havuzu oluştur
    with Pool(processes=num_processes) as pool:
        # Tüm veri tiplerini paralel olarak işle
        pool.map(process_data_type, data_configs)

if __name__ == "__main__":
    main()