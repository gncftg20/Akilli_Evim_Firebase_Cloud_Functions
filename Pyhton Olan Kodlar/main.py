<<<<<<< HEAD
from firebase_functions import scheduler_fn, https_fn,options
from firebase_admin import initialize_app, db, messaging
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
import statistics
import os
import logging

initialize_app()


WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY', '23df2dc3b2bc2307ba172e629ca2772b')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@scheduler_fn.on_schedule(schedule="0 3 * * *")  
def aliskanlik_ogren(event):
    """
    Günlük çalışır.
    1. Evin ısınma hızını hesaplar.
    2. Cihazların ve kombinin kullanım istatistiklerini çıkarır.
    3. Haftalık pattern analizine veri sağlar.
    """
    logger.info("🧠 Alışkanlık ve İstatistikler hesaplanıyor...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    if not users:
        return
    
    for user_id, user_data in users.items():
        try:
            dataset = user_data.get('ai', {}).get('dataset', {})
            
            if not dataset or len(dataset) < 20:
                continue
            
            
            data_list = []
            for d in dataset.values():
                try:
                    ts_str = d.get('sourceTimestamp', '0')
                    ts = int(ts_str) if isinstance(ts_str, (int, str)) else 0
                    if ts > 0:
                        item = {
                            'ts': ts,
                            'kombiDurum': d.get('kombiDurum'),
                            'temp': float(d.get('currentTemp', 0)),
                            'kombiOn': d.get('kombiOn', 0) == 1,
                            'target': float(d.get('targetTemp', 0) or 0),
                            'hour': d.get('hour', 0),
                            
                            'device1': d.get('device1', 0) == 1,
                            'device2': d.get('device2', 0) == 1,
                            'device3': d.get('device3', 0) == 1,
                            'device4': d.get('device4', 0) == 1
                        }
                        data_list.append(item)
                except (ValueError, TypeError):
                    continue
            
            
            data_list.sort(key=lambda x: x['ts'])
            
            
            seven_days_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
            recent = [d for d in data_list if d['ts'] >= seven_days_ago]
            
            if len(recent) < 20:
                continue
            
            
            kombi_acilis_saatleri = []
            tercih_sicakliklar = []
            
            
            stats = {
                'kombi': {'total_hours': 0, 'peak_hours': [0]*24, 'cycles': 0},
                'device1': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device2': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device3': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device4': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []}
            }
            
            last_kombi_state = None
            last_device_states = {1: None, 2: None, 3: None, 4: None}
            
            for i, d in enumerate(recent):
                
                
                duration_hours = 0
                if i < len(recent) - 1:
                    diff = recent[i+1]['ts'] - d['ts']
                    if diff < 3600: 
                        duration_hours = diff / 3600.0
                else:
                    duration_hours = 10 / 60.0 

                hour = d['hour']
                kombi_on = d['kombiOn']
                target = d['target']
                
                
                raw_durum = d.get('kombiDurum')
                if raw_durum is not None:
                    if isinstance(raw_durum, str):
                        kombi_durum = raw_durum.lower() in ('true', '1', 'on', 'aktif')
                    else:
                        kombi_durum = bool(raw_durum)
                else:
                    kombi_durum = kombi_on and (target > 0 and d['temp'] < target)

                d['kombiDurum'] = 1 if kombi_durum else 0 

                
                if kombi_durum:
                    stats['kombi']['total_hours'] += duration_hours
                    stats['kombi']['peak_hours'][hour] += duration_hours 
                
                if last_kombi_state is False and kombi_on:
                    kombi_acilis_saatleri.append(hour)
                    stats['kombi']['cycles'] += 1
                last_kombi_state = kombi_on

                if target > 0: tercih_sicakliklar.append(target)
                
                
                for dev_idx in range(1, 5):
                    dev_key = f'device{dev_idx}'
                    is_on = d[dev_key]
                    
                    if is_on:
                        stats[dev_key]['total_hours'] += duration_hours
                    
                    
                    if last_device_states[dev_idx] is not None:
                        if not last_device_states[dev_idx] and is_on: 
                            stats[dev_key]['on_count'] += 1
                            stats[dev_key]['avg_on_hour'].append(hour + (datetime.fromtimestamp(d['ts']).minute / 60.0))
                        elif last_device_states[dev_idx] and not is_on: 
                             stats[dev_key]['avg_off_hour'].append(hour + (datetime.fromtimestamp(d['ts']).minute / 60.0))
                    
                    last_device_states[dev_idx] = is_on

            
            
            heating_rates = []
            session_start = None
            for i, d in enumerate(data_list): 
                if d['kombiDurum'] and session_start is None:
                    session_start = d
                elif (not d['kombiDurum'] or (i > 0 and d['ts'] - data_list[i-1]['ts'] > 1800)) and session_start is not None:
                    duration_seconds = data_list[i-1]['ts'] - session_start['ts']
                    start_temp = session_start['temp']
                    end_temp = data_list[i-1]['temp']
                    if duration_seconds >= 1200 and end_temp > start_temp + 0.3:
                        duration_hours = duration_seconds / 3600
                        rate = (end_temp - start_temp) / duration_hours
                        if 0.5 <= rate <= 5.0: heating_rates.append(rate)
                    session_start = None
            
            avg_heating_rate = 0
            if heating_rates:
                avg_heating_rate = sum(heating_rates) / len(heating_rates)
                avg_heating_rate = round(avg_heating_rate, 2)
                logger.info(f"🔥 {user_id}: Ortalama ısınma hızı: {avg_heating_rate} °C/Saat")

            
            
            
            peak_hours_indices = sorted(range(len(stats['kombi']['peak_hours'])), key=lambda i: stats['kombi']['peak_hours'][i], reverse=True)[:3]
            
            final_stats = {
                'guncelleme': datetime.now(timezone.utc).isoformat(),
                'isinma_hizi': avg_heating_rate if avg_heating_rate > 0 else 1.5,
                'kombi': {
                    'toplam_calisma_saat_7gun': round(stats['kombi']['total_hours'], 1),
                    'gunluk_ort_saat': round(stats['kombi']['total_hours'] / 7.0, 1),
                    'yogun_saatler': peak_hours_indices,
                    'dongu_sayisi': stats['kombi']['cycles']
                }
            }
            
            
            for dev_idx in range(1, 5):
                dev_key = f'device{dev_idx}'
                s = stats[dev_key]
                on_hours = s['avg_on_hour']
                off_hours = s['avg_off_hour']
                
                final_stats[dev_key] = {
                    'toplam_calisma_saat_7gun': round(s['total_hours'], 1),
                    'gunluk_ort_saat': round(s['total_hours'] / 7.0, 1),
                    'ort_acilis': round(sum(on_hours)/len(on_hours), 1) if on_hours else None,
                    'ort_kapanis': round(sum(off_hours)/len(off_hours), 1) if off_hours else None,
                    'acilis_sayisi': s['on_count']
                }

            if kombi_acilis_saatleri:
                en_sik = max(set(kombi_acilis_saatleri), key=kombi_acilis_saatleri.count)
                final_stats['sabah_acilis_saati'] = en_sik
            
            if tercih_sicakliklar:
                final_stats['tercih_sicaklik'] = round(statistics.mean(tercih_sicakliklar), 1)
            
            
            db.reference(f'users/{user_id}/ai/aliskanlik').set(final_stats)
            
            
            logger.info(f"✅ {user_id}: İstatistik analizi tamamlandı.")
            
        except Exception as e:
            logger.error(f"❌ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 15 minutes", memory=512)
def kombi_performans_kontrol(event):
    """
    Kombi açıkken ısınma/soğuma performansını kontrol eder
    """
    logger.info("🔥 Kombi performans kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            kombi_on = termostat.get('kombionoff', False)    
            mevcut = termostat.get('mevcutderece', 20)
            hedef = termostat.get('hedefderece', 23)
            
            
            
            
            
            real_kombi_durum = termostat.get('kombiDurum') 
            
            if real_kombi_durum is not None:
                
                if isinstance(real_kombi_durum, str):
                    kombi_durum = real_kombi_durum.lower() in ('true', '1', 'on', 'aktif')
                else:
                    kombi_durum = bool(real_kombi_durum)
            else:
                
                kombi_durum = kombi_on and (mevcut < hedef)
            
            if not kombi_durum:  
                continue
            
            
            dataset = user_data.get('ai', {}).get('dataset', {})
            otuz_dk_once = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp())
            recent = []
            for d in dataset.values():
                try:
                    ts = int(d.get('sourceTimestamp', '0'))
                    
                    if ts >= otuz_dk_once and d.get('kombiDurum') == 1:
                        recent.append(d)
                except (ValueError, TypeError):
                    continue
            
            if len(recent) < 6:  
                continue
            
            
            if mevcut < hedef - 2:  
                ilk_sicaklik = recent[0].get('currentTemp', mevcut)
                if mevcut <= ilk_sicaklik + 0.5:  
                    gonder_akilli_bildirim(user_id,
                        "⚠️ Ev Isınmıyor",
                        f"Kombi 30 dakikadır açık ama {mevcut:.1f}°C'de kalıyor. Kontrol et.",
                        {
                            'action': 'turn_off'
                        })
                    logger.warning(f"   🚨 {user_id}: Isınma sorunu")
            
            
            elif mevcut > hedef + 2:  
                gonder_akilli_bildirim(user_id,
                    "🔥 Fazla Sıcak",
                    f"Hedef {hedef:.1f}°C ama {mevcut:.1f}°C. Başka ısı kaynağı var mı?",
                    {
                        'action': 'turn_off'
                    })
                logger.warning(f"   🚨 {user_id}: Aşırı ısınma")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 30 minutes")
def evden_uzaklik_kontrol(event):
    """
    Kullanıcı evden uzun süre uzaksa ve kombi açıksa öneride bulunur
    """
    logger.info("🏠 Uzaklık kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    simdi = datetime.now(timezone.utc).timestamp()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            home_away = user_data.get('ai', {}).get('home_away', {})
            if home_away.get('status') != 'AWAY':
                continue
            
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            kombi_on = termostat.get('kombionoff', False)
            
            if not kombi_on:
                continue
            
            
            away_timestamp = home_away.get('timestamp', simdi)
            away_duration = (simdi - away_timestamp) / 3600  
            
            if away_duration >= 2:
                mevcut = termostat.get('mevcutderece', 20)
                yeni_hedef = max(mevcut - 2, 18)  
                
                yeni_hedef = round(yeni_hedef, 1)
                
                
                last_notification = user_data.get('ai', {}).get('last_away_notification', 0)
                if simdi - last_notification > 7200:  
                    gonder_akilli_bildirim(user_id,
                        "💡 Enerji Tasarrufu",
                        f"2 saattir evde değilsin. Kombi kapatılsın mı veya hedef {yeni_hedef:.1f}°C'ye düşürülsün mü?",
                        {
                            'action': 'away_mode',
                            'new_target': str(yeni_hedef)  
                        })
                    
                    db.reference(f'users/{user_id}/ai/last_away_notification').set(int(simdi))
                    logger.info(f"   📲 {user_id}: Uzaklık önerisi")
                    
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 3 hours")
def hava_durumu_kontrol(event):
    """
    Hava soğuyacaksa ve kullanıcı dışarıdaysa, eve geldiğinde sıcak olması için önerir
    """
    logger.info("❄️ Hava durumu kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            home_away = user_data.get('ai', {}).get('home_away', {})
            is_away = home_away.get('status') == 'AWAY'
            
            if not is_away:
                continue
            
            
            home_loc = user_data.get('ai', {}).get('home_location', {})
            lat = home_loc.get('lat', 40.1435)
            lon = home_loc.get('lon', 29.9811)
            
            
            try:
                url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric&lang=tr"
                response = requests.get(url, timeout=10)
                response.raise_for_status()  
                weather = response.json()
                
                
                if 'list' not in weather or len(weather['list']) < 3:
                    logger.warning(f"   ⚠️ {user_id}: Hava durumu verisi yetersiz")
                    continue
                
                
                now_temp = weather['list'][0]['main']['temp']
                future_temp = weather['list'][2]['main']['temp']
                diff = now_temp - future_temp
            except requests.exceptions.RequestException as e:
                logger.error(f"   ❌ {user_id}: Hava durumu API hatası - {str(e)}")
                continue
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"   ❌ {user_id}: Hava durumu veri parse hatası - {str(e)}")
                continue
            
            
            if diff >= 4:
                termostat = user_data.get('Kontrol', {}).get('termostat', {})
                hedef = termostat.get('hedefderece', 23)
                
                if isinstance(hedef, float):
                    hedef = round(hedef, 1)
                yeni_hedef = min(hedef + 2, 26)
                
                yeni_hedef = round(yeni_hedef, 1)
                
                gonder_akilli_bildirim(user_id,
                    "❄️ Hava Soğuyacak",
                    f"6 saatte {diff:.0f}°C soğuma bekleniyor. Eve geldiğinde sıcak olması için hedef {yeni_hedef:.1f}°C yapılsın mı?",
                    {
                        'action': 'weather_heat',
                        'new_target': str(yeni_hedef)  
                    })
                logger.info(f"   📲 {user_id}: Hava uyarısı (+2°C önerisi)")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 15 minutes", memory=512)
def kritik_durum_kontrol(event):
    """
    Cihaz offline ve donma riski kontrolü
    """
    logger.info("🚨 Kritik kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    simdi = datetime.now(timezone.utc).timestamp()
    
    for user_id, user_data in users.items():
        try:
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            son_guncelleme = termostat.get('son_guncelleme')
            
            
            if son_guncelleme:
                try:
                    if isinstance(son_guncelleme, str):
                        
                        if 'T' in son_guncelleme or 'Z' in son_guncelleme:
                            son_guncelleme_ts = datetime.fromisoformat(son_guncelleme.replace('Z', '+00:00')).timestamp()
                        else:
                            
                            son_guncelleme_ts = float(son_guncelleme)
                    elif isinstance(son_guncelleme, (int, float)):
                        son_guncelleme_ts = float(son_guncelleme)
                    else:
                        continue
                    
                    
                    if simdi - son_guncelleme_ts > 900:
                        
                        gonder_bildirim(user_id,
                            "❌ Cihaz Offline",
                            "15 dakikadır veri gelmiyor. İnternet bağlantısını kontrol et.")
                        logger.warning(f"   🚨 {user_id}: Offline")
                        continue
                    else:
                        
                        
                        
                        pass
                except (ValueError, TypeError, AttributeError) as e:
                    logger.debug(f"   ⚠️ {user_id}: Timestamp parse hatası - {str(e)}")
                    continue
            
            
            sicaklik = termostat.get('mevcutderece', 20)
            if sicaklik < 12:
                
                
                ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
                if ai_settings.get('notify', True):  
                    gonder_akilli_bildirim(user_id,
                        "🥶 DONMA RİSKİ",
                        f"Sıcaklık {sicaklik:.1f}°C'ye düştü! Kombiyi hemen aç.",
                        {
                            'action': 'heat_emergency'
                        })
                    logger.warning(f"   🚨 {user_id}: Donma riski!")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="0 9 * * 1")  
def haftalik_rapor_gonder(event):
    """
    Geçen haftanın özetini gönderir
    """
    logger.info("📊 Haftalık rapor...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    gecen_hafta = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-W%U')
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            
            haftalik_log = user_data.get('ai', {}).get('haftalik_log', {})
            gecen_hafta_data = haftalik_log.get(gecen_hafta)
            
            if not gecen_hafta_data:
                continue
            
            calisma_saat = gecen_hafta_data.get('toplam_calisma_saat', 0)
            
            
            onceki_hafta = (datetime.now(timezone.utc) - timedelta(days=14)).strftime('%Y-W%U')
            onceki_hafta_data = haftalik_log.get(onceki_hafta)
            
            mesaj = f"📊 Geçen hafta: {calisma_saat}h kombi çalıştı"
            
            if onceki_hafta_data:
                onceki_saat = onceki_hafta_data.get('toplam_calisma_saat', 0)
                if onceki_saat > 0:
                    fark = ((onceki_saat - calisma_saat) / onceki_saat) * 100
                    if abs(fark) > 5:  
                        if fark > 0:
                            mesaj += f"\n🌱 Önceki haftaya göre %{fark:.0f} tasarruf!"
                        else:
                            mesaj += f"\n📈 Önceki haftaya göre %{abs(fark):.0f} fazla kullanım"
            
            gonder_bildirim(user_id, "📊 Haftalık Özet", mesaj)
            logger.info(f"   📊 {user_id}: Rapor gönderildi")
            
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")



logger = logging.getLogger('cloud_functions')

@scheduler_fn.on_schedule(schedule="*/10 * * * *", memory=1024)
def programi_uygula(event):
    """
    TR Saati ile çalışır. 10 dakikada bir tetiklenir.
    Yeni yapıya göre (target_temp ve devices map) çalışır.
    """
    
    utc_now = datetime.now(timezone.utc)
    tr_now = utc_now + timedelta(hours=3) 
    
    current_day = tr_now.weekday()  
    current_hour = tr_now.hour
    raw_minute = tr_now.minute
    
    current_minute = (raw_minute // 10) * 10 
    
    current_time_minutes = current_hour * 60 + current_minute

    logger.info(f"⏰ Program Kontrolü Başladı - TR Saati: {tr_now.strftime('%Y-%m-%d %H:%M')} (Gün: {current_day})")

    try:
        users_ref = db.reference('users')
        users = users_ref.get()

        if not users:
            logger.info("Kullanıcı bulunamadı.")
            return

        for user_id, user_data in users.items():
            
            program_root = user_data.get('Program')
            if not program_root or not isinstance(program_root, dict):
                continue

            
            home_away_status = user_data.get('ai', {}).get('home_away', {}).get('status', 'UNKNOWN')
            
            
            for prog_id, prog_data in program_root.items():
                
                if prog_id in ['onay_bekleyen', 'weekly', 'location'] or not isinstance(prog_data, dict):
                    continue

                
                if not prog_data.get('enabled', False):
                    continue

                
                
                
                if 'day' in prog_data:
                    
                    if prog_data.get('day') != current_day:
                        continue 

                    open_hour = prog_data.get('open_hour', 0)
                    open_minute = (prog_data.get('open_minute', 0) // 10) * 10
                    close_hour = prog_data.get('close_hour', 0)
                    close_minute = (prog_data.get('close_minute', 0) // 10) * 10
                    
                    start_mins = open_hour * 60 + open_minute
                    end_mins = close_hour * 60 + close_minute
                    
                    start_mins = open_hour * 60 + open_minute
                    end_mins = close_hour * 60 + close_minute

                    
                    
                    minutes_until_start = start_mins - current_time_minutes
                    
                    
                    if 0 < minutes_until_start <= 120 and 'target_temp' in prog_data:
                        
                        isinma_hizi = user_data.get('ai', {}).get('aliskanlik', {}).get('isinma_hizi', 1.5) 
                        mevcut_temp = user_data.get('Kontrol', {}).get('termostat', {}).get('mevcutderece', 20.0)
                        target_temp = float(prog_data['target_temp'])
                        
                        if mevcut_temp < target_temp:
                            
                            temp_diff = target_temp - mevcut_temp
                            if isinma_hizi > 0:
                                needed_hours = temp_diff / isinma_hizi
                                needed_minutes = needed_hours * 60
                                
                                
                                
                                if needed_minutes >= (minutes_until_start - 10):
                                    logger.info(f"🔥 {user_id}: PRE-HEATING Aktif! Program: {prog_id} Hedef: {target_temp}°C Fark: {temp_diff}°C Süre: {int(needed_minutes)}dk")
                                    
                                    
                                    db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                        'hedefderece': target_temp,
                                        'ekomod': False, 
                                        'kombionoff': True
                                    })
                                    
                                    continue
                    if current_hour == open_hour and current_minute == open_minute:
                        logger.info(f"✅ {user_id}: Program BAŞLATILIYOR ({prog_id})")
                        
                        
                        devices = prog_data.get('devices')
                        if devices and isinstance(devices, dict):
                            for dev_name, state in devices.items():
                                
                                db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(state)

                        
                        if 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                            target = float(prog_data['target_temp'])
                            db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                'hedefderece': target,
                                'ekomod': prog_data.get('eko_mode', True),
                                'kombionoff': True
                            })

                    
                    elif current_hour == close_hour and current_minute == close_minute:
                        logger.info(f"🛑 {user_id}: Program SONLANDIRILIYOR ({prog_id})")
                        
                        
                        devices = prog_data.get('devices')
                        if devices and isinstance(devices, dict):
                            for dev_name in devices.keys():
                                
                                db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(False)

                        
                        if 'target_temp' in prog_data:
                            db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                'hedefderece': 23.0, 
                                'kombionoff': True   
                            })

                    
                    
                    elif 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                        in_range = False
                        if start_mins < end_mins:
                            in_range = start_mins <= current_time_minutes < end_mins
                        else: 
                            in_range = current_time_minutes >= start_mins or current_time_minutes < end_mins
                        
                        if in_range:
                            target = float(prog_data['target_temp'])
                            
                            current_val = user_data.get('Kontrol', {}).get('termostat', {}).get('hedefderece')
                            if current_val != target:
                                db.reference(f'users/{user_id}/Kontrol/termostat/hedefderece').set(target)

                
                
                
                elif 'trigger' in prog_data:
                    trigger_type = prog_data.get('trigger') 
                    
                    
                    last_trigger = user_data.get('ai', {}).get('home_away', {}).get('last_program_trigger_time', 0)
                    status_ts = user_data.get('ai', {}).get('home_away', {}).get('timestamp', 0)
                    
                    
                    if status_ts > last_trigger:
                        should_run = False
                        if trigger_type == 'leave' and home_away_status == 'AWAY':
                            should_run = True
                        elif trigger_type == 'arrive' and home_away_status == 'HOME':
                            should_run = True
                            
                        if should_run:
                            logger.info(f"📍 {user_id}: Konum Programı Tetikleniyor ({trigger_type})")
                            
                            
                            devices = prog_data.get('devices')
                            if devices and isinstance(devices, dict):
                                for dev_name, state in devices.items():
                                    
                                    db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(state)
                                    
                            
                            if 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                                target = float(prog_data['target_temp'])
                                db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                    'hedefderece': target,
                                    'ekomod': True,
                                    'kombionoff': True
                                })

                            
                            db.reference(f'users/{user_id}/ai/home_away/last_program_trigger_time').set(int(datetime.now().timestamp()))

    except Exception as e:
        logger.error(f"Program döngüsünde hata: {str(e)}")


logger = logging.getLogger('cloud_functions')

class Config:
    MIN_REQUIRED_DATA = 20
    MIN_RECENT_DATA = 14
    DAYS_TO_ANALYZE = 7
    MIN_PATTERN_COUNT = 2
    PATTERN_CONFIDENCE_THRESHOLD = 0.5
    DEFAULT_OPENING_HOUR = 7
    DEFAULT_TEMP = 23.0
    MIN_TEMP = 18.0
    MAX_TEMP = 30.0
    HOURS_IN_DAY = 24
    DAYS_IN_WEEK = 7






@dataclass
class PatternData:
    kombi_acik_sayisi: int = 0
    kombi_toplam: int = 0
    hedef_sicakliklar: List[float] = None
    cihazlar: Dict[str, int] = None
    
    def __post_init__(self):
        if self.hedef_sicakliklar is None:
            self.hedef_sicakliklar = []
        if self.cihazlar is None:
            self.cihazlar = defaultdict(int)


@dataclass
class GuvenilirPattern:
    kombi_acik: bool = False
    hedef_sicaklik: float = Config.DEFAULT_TEMP
    cihazlar: Set[str] = None
    
    def __post_init__(self):
        if self.cihazlar is None:
            self.cihazlar = set()


@dataclass
class ZamanAraligi:
    baslangic: int
    bitis: int
    kombi_acik: bool
    hedef_sicaklik: float
    cihazlar: List[str]
    
    def to_dict(self) -> Dict:
        
        summary = {
            'baslangic_saat': self.baslangic,
            'bitis_saat': self.bitis,
            'kombi_acik': self.kombi_acik,
        }
        if self.kombi_acik:
            summary['hedef_sicaklik'] = self.hedef_sicaklik
        
        if self.cihazlar:
            
            summary['cihazlar'] = self.cihazlar
            
        return summary






class DataValidator:
   
    @staticmethod
    def parse_timestamp(data: Dict) -> int:
        try:
            ts_str = data.get('sourceTimestamp', '0')
            return int(ts_str) if isinstance(ts_str, (int, str)) else 0
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def validate_hour(hour: int) -> bool:
        return isinstance(hour, int) and 0 <= hour < Config.HOURS_IN_DAY
    
    @staticmethod
    def validate_temp(temp: float) -> bool:
        return temp and Config.MIN_TEMP <= temp <= Config.MAX_TEMP
    
    @staticmethod
    def validate_day(day: int) -> bool:
        return isinstance(day, int) and 0 <= day < Config.DAYS_IN_WEEK


class DatasetAnalyzer:
    
    def __init__(self, dataset: Dict):
        self.dataset = dataset
        self.validator = DataValidator()
    
    def get_recent_data(self, days: int = Config.DAYS_TO_ANALYZE) -> List[Dict]:
        cutoff_timestamp = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        )
        recent = []
        for data in self.dataset.values():
            ts = self.validator.parse_timestamp(data)
            if ts >= cutoff_timestamp:
                recent.append(data)
        return recent
    
    def analyze_opening_hours(self, recent_data: List[Dict]) -> Optional[int]:
        opening_hours = []
        last_state = None
        sorted_data = sorted(recent_data, key=lambda x: self.validator.parse_timestamp(x))
        
        for data in sorted_data:
            hour = data.get('hour', 0)
            if not self.validator.validate_hour(hour): continue
            kombi_on = data.get('kombiOn', 0) == 1
            if last_state is False and kombi_on:
                opening_hours.append(hour)
            last_state = kombi_on
            
        if not opening_hours: return None
        return max(set(opening_hours), key=opening_hours.count)
    
    def analyze_preferred_temp(self, recent_data: List[Dict]) -> Optional[float]:
        temps = []
        for data in recent_data:
            target = data.get('targetTemp', 0)
            if self.validator.validate_temp(target):
                temps.append(float(target))
        if not temps: return None
        return round(statistics.mean(temps), 1)
    
    def analyze_user_habits(self, aliskanlik: Dict, recent_data: List[Dict]) -> Tuple[int, float]:
        if aliskanlik.get('sabah_acilis_saati'):
            return aliskanlik.get('sabah_acilis_saati'), aliskanlik.get('tercih_sicaklik', Config.DEFAULT_TEMP)
        
        acilis_saati = self.analyze_opening_hours(recent_data) or Config.DEFAULT_OPENING_HOUR
        tercih_sicaklik = self.analyze_preferred_temp(recent_data) or Config.DEFAULT_TEMP
        return acilis_saati, tercih_sicaklik


class PatternAnalyzer:

    def __init__(self, dataset: Dict):
        self.dataset = dataset
        self.validator = DataValidator()
    
    def build_daily_patterns(self, recent_data: List[Dict]) -> Dict[int, Dict[int, PatternData]]:
        patterns = defaultdict(lambda: defaultdict(lambda: PatternData(hedef_sicakliklar=[], cihazlar=defaultdict(int))))
        
        for data in recent_data:
            ts = self.validator.parse_timestamp(data)
            if ts == 0: continue
            
            tarih = datetime.fromtimestamp(ts, tz=timezone.utc)
            gun = tarih.weekday()
            hour = data.get('hour', -1)
            
            if not self.validator.validate_hour(hour): continue
            
            pattern = patterns[gun][hour]
            pattern.kombi_toplam += 1
            if data.get('kombiDurum', 0) == 1:
                pattern.kombi_acik_sayisi += 1
            
            target = data.get('targetTemp', 0)
            if self.validator.validate_temp(target):
                pattern.hedef_sicakliklar.append(float(target))
            
            
            for i in range(1, 5):
                if data.get(f'device{i}', 0) == 1:
                    pattern.cihazlar[f'cihaz{i}'] += 1
        
        return patterns
    
    def extract_reliable_patterns(self, daily_patterns: Dict, default_temp: float) -> Dict:
        reliable = defaultdict(lambda: defaultdict(lambda: GuvenilirPattern(hedef_sicaklik=default_temp, cihazlar=set())))
        
        for gun in range(Config.DAYS_IN_WEEK):
            for saat in range(Config.HOURS_IN_DAY):
                if saat not in daily_patterns[gun]: continue
                pattern = daily_patterns[gun][saat]
                
                if pattern.kombi_toplam >= Config.MIN_PATTERN_COUNT:
                    reliable_pattern = reliable[gun][saat]
                    oran = pattern.kombi_acik_sayisi / pattern.kombi_toplam
                    if oran > Config.PATTERN_CONFIDENCE_THRESHOLD:
                        reliable_pattern.kombi_acik = True
                        if pattern.hedef_sicakliklar:
                            reliable_pattern.hedef_sicaklik = round(statistics.mean(pattern.hedef_sicakliklar), 1)
                    
                    for cihaz, kullanim in pattern.cihazlar.items():
                        if kullanim >= Config.MIN_PATTERN_COUNT:
                            reliable_pattern.cihazlar.add(cihaz)
        return reliable


class TimeIntervalBuilder:
   
    @staticmethod
    def create_intervals(reliable_patterns: Dict[int, GuvenilirPattern]) -> List[ZamanAraligi]:
        intervals = []
        baslangic_saat = None
        onceki_ozellik = None
        
        for saat in range(Config.HOURS_IN_DAY):
            
            if saat not in reliable_patterns:
                if baslangic_saat is not None:
                    intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                    baslangic_saat = None
                    onceki_ozellik = None
                continue
            
            pattern = reliable_patterns[saat]
            
            mevcut_ozellik = (
                pattern.kombi_acik,
                pattern.hedef_sicaklik,
                tuple(sorted(pattern.cihazlar))
            )
            
            
            if not pattern.kombi_acik and not pattern.cihazlar:
                if baslangic_saat is not None:
                    intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                    baslangic_saat = None
                    onceki_ozellik = None
                continue

            if onceki_ozellik is None:
                baslangic_saat = saat
                onceki_ozellik = mevcut_ozellik
            elif mevcut_ozellik != onceki_ozellik:
                
                intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                baslangic_saat = saat
                onceki_ozellik = mevcut_ozellik
        
        if baslangic_saat is not None:
            intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, Config.HOURS_IN_DAY, onceki_ozellik))
            
        return intervals
    
    @staticmethod
    def _create_interval(baslangic: int, bitis: int, ozellik: Tuple) -> ZamanAraligi:
        return ZamanAraligi(
            baslangic=baslangic,
            bitis=bitis,
            kombi_acik=ozellik[0],
            hedef_sicaklik=ozellik[1],
            cihazlar=list(ozellik[2])
        )


class ProgramGenerator:
    
    def __init__(self, user_id: str):
        self.user_id = user_id
    
    def generate_programs(self, intervals_by_day: Dict[int, List[ZamanAraligi]], default_temp: float, 
                         include_temp: bool = True, allowed_devices: List[str] = None) -> Tuple[Dict, List[Dict]]:
        
        program_updates = {}
        interval_summaries = []
        
        for day, intervals in intervals_by_day.items():
            if not intervals: continue
            
            
            
            
            
            filtered_intervals = []
            for interval in intervals:
                
                if not include_temp:
                    interval.kombi_acik = False
                    interval.hedef_sicaklik = 0
                
                
                if interval.cihazlar and allowed_devices is not None:
                    
                    
                    filtered_devices = [d for d in interval.cihazlar if d in allowed_devices]
                    interval.cihazlar = filtered_devices
                
                
                if not interval.kombi_acik and not interval.cihazlar:
                    continue
                    
                filtered_intervals.append(interval)

            if not filtered_intervals:
                continue

            
            merged_intervals = self._merge_similar_intervals(filtered_intervals, max_gap=1)
            
            logger.info(f"📅 Gün {day}: {len(merged_intervals)} program oluşturuluyor (Filtreli)")

            for item in merged_intervals:
                
                program_data = {
                    'day': day,
                    'open_hour': item['start'],
                    'open_minute': 0,
                    'close_hour': item['end'],
                    'close_minute': 0,
                    'enabled': False,
                    'source': 'ai',
                }
                
                
                if item['kombi_acik']:
                    program_data['target_temp'] = item['temp']
                    program_data['eko_mode'] = True
                else:
                    program_data['target_temp'] = None
                    program_data['eko_mode'] = None
                    
                
                if item['devices']:
                    program_data['devices'] = item['devices']
                else:
                    program_data['devices'] = None
                    
                
                if program_data['target_temp'] is None and program_data['devices'] is None:
                    continue

                program_ref = db.reference(f'users/{self.user_id}/Program').push()
                program_id = program_ref.key
                
                if program_id:
                    program_updates[f'Program/{program_id}'] = program_data
                    
                    
                    log_msg = f"✅ Prog: {item['start']}-{item['end']}"
                    if item['kombi_acik']: log_msg += f" | 🌡️ {item['temp']}°C"
                    if item['devices']: log_msg += f" | 🔌 {list(item['devices'].keys())}"
                    logger.info(log_msg)

            
            for interval in filtered_intervals:
                interval_summaries.append(interval.to_dict())

        
        
        
        
        if include_temp:
            location_ref = db.reference(f'users/{self.user_id}/Program').push()
            location_id = location_ref.key
            if location_id:
                location_program = {
                    'trigger': 'arrive',
                    'target_temp': default_temp,
                    'enabled': False,
                    'source': 'ai',
                    'devices': None 
                }
                program_updates[f'Program/{location_id}'] = location_program
        
        return program_updates, interval_summaries

    def _merge_similar_intervals(self, intervals: List[ZamanAraligi], max_gap: int = 1) -> List[Dict]:
        if not intervals: return []
        
        sorted_intervals = sorted(intervals, key=lambda x: x.baslangic)
        merged = []
        
        
        first = sorted_intervals[0]
        current = {
            'start': first.baslangic,
            'end': first.bitis,
            'kombi_acik': first.kombi_acik,
            'temp': first.hedef_sicaklik,
            'devices': {dev: True for dev in first.cihazlar} if first.cihazlar else {} 
        }
        
        for interval in sorted_intervals[1:]:
            
            
            i_kombi = interval.kombi_acik
            i_temp = interval.hedef_sicaklik
            i_devices = {dev: True for dev in interval.cihazlar} if interval.cihazlar else {}
            
            gap = interval.baslangic - current['end']
            
            is_same_config = (
                current['kombi_acik'] == i_kombi and
                abs(current['temp'] - i_temp) < 0.5 and 
                set(current['devices'].keys()) == set(i_devices.keys()) 
            )
            
            if gap <= max_gap and is_same_config:
                current['end'] = max(current['end'], interval.bitis)
            else:
                merged.append(current)
                
                current = {
                    'start': interval.baslangic,
                    'end': interval.bitis,
                    'kombi_acik': i_kombi,
                    'temp': i_temp,
                    'devices': i_devices
                }
        
        merged.append(current)
        return merged






@https_fn.on_call(
    cors=options.CorsOptions(cors_origins="*", cors_methods=["get", "post"]),
    memory=options.MemoryOption.MB_512,
    timeout_sec=120
)
def program_olustur(req: https_fn.CallableRequest) -> Dict:
    if not req.auth:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Kimlik doğrulama gerekli')
    
    user_id = req.auth.uid
    
    
    include_temp = req.data.get('include_temp', True)
    allowed_devices = req.data.get('allowed_devices') 
    
    
    try:
        logger.info(f"🔄 Program oluşturma başladı: {user_id} | Filtre: Temp={include_temp}, Devs={allowed_devices}")
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.NOT_FOUND, message='Kullanıcı verisi bulunamadı')
        
        dataset = user_data.get('ai', {}).get('dataset', {})
        aliskanlik = user_data.get('ai', {}).get('aliskanlik', {})
        
        
        if not dataset or len(dataset) < Config.MIN_REQUIRED_DATA:
            return {'success': False, 'message': f'Yeterli veri yok ({len(dataset) if dataset else 0}/{Config.MIN_REQUIRED_DATA})'}
        
        
        analyzer = DatasetAnalyzer(dataset)
        recent_data = analyzer.get_recent_data()
        
        if len(recent_data) < Config.MIN_RECENT_DATA:
            return {'success': False, 'message': f'Son {Config.DAYS_TO_ANALYZE} günde yeterli veri yok.'}
        
        acilis_saati, tercih_sicaklik = analyzer.analyze_user_habits(aliskanlik, recent_data)
        
        pattern_analyzer = PatternAnalyzer(dataset)
        daily_patterns = pattern_analyzer.build_daily_patterns(recent_data)
        reliable_patterns = pattern_analyzer.extract_reliable_patterns(daily_patterns, tercih_sicaklik)
        
        intervals_by_day = {}
        for day in range(Config.DAYS_IN_WEEK):
            intervals = TimeIntervalBuilder.create_intervals(reliable_patterns[day])
            if intervals: intervals_by_day[day] = intervals
        
        
        program_gen = ProgramGenerator(user_id)
        program_updates, interval_summaries = program_gen.generate_programs(
            intervals_by_day, 
            tercih_sicaklik,
            include_temp=include_temp,
            allowed_devices=allowed_devices
        )
        
        if program_updates:
             
            db.reference(f'users/{user_id}').update(program_updates)
            
            
            onay_bekleyen_data = {
                'aktif': False,
                'olusturma_zamani': datetime.now(timezone.utc).isoformat(),
                'acilis_saati': acilis_saati,
                'tercih_sicaklik': tercih_sicaklik,
                'program_count': len(program_updates),
                'araliklar': interval_summaries
            }
            db.reference(f'users/{user_id}/Program/onay_bekleyen').set(onay_bekleyen_data)
            
            return {
                'success': True,
                'message': 'Program oluşturuldu.',
                'program_count': len(program_updates),
                'generated_programs': True
            }
        else:
            return {'success': False, 'message': 'Kriterlere uygun güvenilir pattern bulunamadı.'} 
    except Exception as e:
        logger.error(f"Hata: {str(e)}")
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))






@https_fn.on_call(cors=options.CorsOptions(cors_origins="*", cors_methods=["post"]), memory=options.MemoryOption.MB_256)
def program_onayla(req: https_fn.CallableRequest) -> Dict:
    if not req.auth: raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Auth required')
    user_id = req.auth.uid
    try:
        
        onay_ref = db.reference(f'users/{user_id}/Program/onay_bekleyen')
        if not onay_ref.get(): return {'success': False, 'message': 'Onay bekleyen yok'}
        
        
        program_ref = db.reference(f'users/{user_id}/Program')
        all_programs = program_ref.get()
        updates = {}
        count = 0
        
        if all_programs:
            for pid, pdata in all_programs.items():
                if isinstance(pdata, dict) and pdata.get('source') == 'ai':
                    
                    if not pdata.get('enabled'):
                        updates[f'Program/{pid}/enabled'] = True
                        count += 1
        
        updates['Program/onay_bekleyen/aktif'] = True
        db.reference(f'users/{user_id}').update(updates)
        return {'success': True, 'message': f'{count} program aktif edildi', 'activated_count': count}
    except Exception as e:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))

@https_fn.on_call(cors=options.CorsOptions(cors_origins="*", cors_methods=["post"]), memory=options.MemoryOption.MB_256)
def program_reddet(req: https_fn.CallableRequest) -> Dict:
    if not req.auth: raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Auth required')
    user_id = req.auth.uid
    try:
        program_ref = db.reference(f'users/{user_id}/Program')
        all_programs = program_ref.get()
        count = 0
        if all_programs:
            for pid, pdata in all_programs.items():
                
                
                if isinstance(pdata, dict) and pdata.get('source') == 'ai' and not pdata.get('enabled', False):
                    db.reference(f'users/{user_id}/Program/{pid}').delete()
                    count += 1
        
        db.reference(f'users/{user_id}/Program/onay_bekleyen').delete()
        return {'success': True, 'message': f'{count} taslak program silindi'}
    except Exception as e:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))



@scheduler_fn.on_schedule(schedule="0 4 * * *")  
def ai_dataset_gunluk_temizlik(event):
    logger.info("🧹 AI Dataset günlük temizlik...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    yirmi_bir_gun_once = int((datetime.now(timezone.utc) - timedelta(days=21)).timestamp())
    toplam_silinen = 0
    
    for user_id, user_data in users.items():
        try:
            dataset = user_data.get('ai', {}).get('dataset', {})
            if not dataset:
                continue
            
            silinen = 0
            for key, data in list(dataset.items()):
                try:
                    
                    ts_str = data.get('sourceTimestamp', '0')
                    if isinstance(ts_str, str):
                        ts = int(ts_str)
                    else:
                        ts = int(ts_str)
                    
                    
                    if ts < yirmi_bir_gun_once:
                        db.reference(f'users/{user_id}/ai/dataset/{key}').delete()
                        silinen += 1
                except (ValueError, TypeError) as e:
                    logger.debug(f"   ⚠️ {user_id}: Dataset kayıt parse hatası - {key}: {str(e)}")
                    continue
            
            if silinen > 0:
                toplam_silinen += silinen
                logger.info(f"   🗑️ {user_id}: {silinen} dataset kaydı silindi")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: Dataset temizlik hatası - {str(e)}")
    
    if toplam_silinen > 0:
        logger.info(f"✅ Toplam {toplam_silinen} dataset kaydı temizlendi (21 günden eski)")
    else:
        logger.info("✅ Temizlenecek eski kayıt bulunamadı")





def gonder_bildirim(user_id, baslik, mesaj):
    try:
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            logger.warning(f"   ⚠️ {user_id}: Kullanıcı bulunamadı")
            return False
        
        fcm_token = user_data.get('fcm_token')
        
        if not fcm_token:
            logger.debug(f"   ⚠️ {user_id}: FCM token yok")
            return False
        
        
        ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
        if not ai_settings.get('notify', True):
            logger.debug(f"   ⚠️ {user_id}: Bildirimler kapalı")
            return False
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=baslik,
                body=mesaj,
            ),
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority='high',
                notification=messaging.AndroidNotification(
                    sound='default'
                )
            )
        )
        
        response = messaging.send(message)
        logger.info(f"   📲 {user_id}: Bildirim gönderildi - {response}")
        return True
        
    except messaging.UnregisteredError:
        logger.warning(f"   ⚠️ {user_id}: FCM token geçersiz, token siliniyor")
        
        try:
            db.reference(f'users/{user_id}/fcm_token').delete()
        except:
            pass
        return False
    except messaging.SenderIdMismatchError:
        logger.error(f"   ❌ {user_id}: FCM sender ID uyuşmazlığı")
        return False
    except Exception as e:
        logger.error(f"   ❌ {user_id}: Bildirim hatası - {str(e)}")
        return False


def gonder_akilli_bildirim(user_id, baslik, mesaj, data):
    
    try:
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            logger.warning(f"   ⚠️ {user_id}: Kullanıcı bulunamadı")
            return False
        
        fcm_token = user_data.get('fcm_token')
        
        if not fcm_token:
            logger.debug(f"   ⚠️ {user_id}: FCM token yok")
            return False
        
        
        ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
        if not ai_settings.get('notify', True):
            logger.debug(f"   ⚠️ {user_id}: Bildirimler kapalı")
            return False
        
        
        
        data_str = {str(k): str(v) for k, v in data.items()}
        data_str['title'] = baslik
        data_str['body'] = mesaj
        data_str['type'] = 'action_notification'  
        
        
        
        message = messaging.Message(
            data=data_str,  
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority='high'
            )
        )
        
        response = messaging.send(message)
        logger.info(f"   📲 {user_id}: Akıllı bildirim gönderildi - {response}")
        return True
        
    except messaging.UnregisteredError:
        logger.warning(f"   ⚠️ {user_id}: FCM token geçersiz, token siliniyor")
        try:
            db.reference(f'users/{user_id}/fcm_token').delete()
        except:
            pass
        return False
    except messaging.SenderIdMismatchError:
        logger.error(f"   ❌ {user_id}: FCM sender ID uyuşmazlığı")
        return False
    except Exception as e:
        logger.error(f"   ❌ {user_id}: Akıllı bildirim hatası - {str(e)}")
        return False
=======
from firebase_functions import scheduler_fn, https_fn,options
from firebase_admin import initialize_app, db, messaging
import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
import statistics
import os
import logging

initialize_app()


WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY', '23df2dc3b2bc2307ba172e629ca2772b')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@scheduler_fn.on_schedule(schedule="0 3 * * *")  
def aliskanlik_ogren(event):
    """
    Günlük çalışır.
    1. Evin ısınma hızını hesaplar.
    2. Cihazların ve kombinin kullanım istatistiklerini çıkarır.
    3. Haftalık pattern analizine veri sağlar.
    """
    logger.info("🧠 Alışkanlık ve İstatistikler hesaplanıyor...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    if not users:
        return
    
    for user_id, user_data in users.items():
        try:
            dataset = user_data.get('ai', {}).get('dataset', {})
            
            if not dataset or len(dataset) < 20:
                continue
            
            
            data_list = []
            for d in dataset.values():
                try:
                    ts_str = d.get('sourceTimestamp', '0')
                    ts = int(ts_str) if isinstance(ts_str, (int, str)) else 0
                    if ts > 0:
                        item = {
                            'ts': ts,
                            'kombiDurum': d.get('kombiDurum'),
                            'temp': float(d.get('currentTemp', 0)),
                            'kombiOn': d.get('kombiOn', 0) == 1,
                            'target': float(d.get('targetTemp', 0) or 0),
                            'hour': d.get('hour', 0),
                            
                            'device1': d.get('device1', 0) == 1,
                            'device2': d.get('device2', 0) == 1,
                            'device3': d.get('device3', 0) == 1,
                            'device4': d.get('device4', 0) == 1
                        }
                        data_list.append(item)
                except (ValueError, TypeError):
                    continue
            
            
            data_list.sort(key=lambda x: x['ts'])
            
            
            seven_days_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())
            recent = [d for d in data_list if d['ts'] >= seven_days_ago]
            
            if len(recent) < 20:
                continue
            
            
            kombi_acilis_saatleri = []
            tercih_sicakliklar = []
            
            
            stats = {
                'kombi': {'total_hours': 0, 'peak_hours': [0]*24, 'cycles': 0},
                'device1': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device2': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device3': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []},
                'device4': {'total_hours': 0, 'on_count': 0, 'avg_on_hour': [], 'avg_off_hour': []}
            }
            
            last_kombi_state = None
            last_device_states = {1: None, 2: None, 3: None, 4: None}
            
            for i, d in enumerate(recent):
                
                
                duration_hours = 0
                if i < len(recent) - 1:
                    diff = recent[i+1]['ts'] - d['ts']
                    if diff < 3600: 
                        duration_hours = diff / 3600.0
                else:
                    duration_hours = 10 / 60.0 

                hour = d['hour']
                kombi_on = d['kombiOn']
                target = d['target']
                
                
                raw_durum = d.get('kombiDurum')
                if raw_durum is not None:
                    if isinstance(raw_durum, str):
                        kombi_durum = raw_durum.lower() in ('true', '1', 'on', 'aktif')
                    else:
                        kombi_durum = bool(raw_durum)
                else:
                    kombi_durum = kombi_on and (target > 0 and d['temp'] < target)

                d['kombiDurum'] = 1 if kombi_durum else 0 

                
                if kombi_durum:
                    stats['kombi']['total_hours'] += duration_hours
                    stats['kombi']['peak_hours'][hour] += duration_hours 
                
                if last_kombi_state is False and kombi_on:
                    kombi_acilis_saatleri.append(hour)
                    stats['kombi']['cycles'] += 1
                last_kombi_state = kombi_on

                if target > 0: tercih_sicakliklar.append(target)
                
                
                for dev_idx in range(1, 5):
                    dev_key = f'device{dev_idx}'
                    is_on = d[dev_key]
                    
                    if is_on:
                        stats[dev_key]['total_hours'] += duration_hours
                    
                    
                    if last_device_states[dev_idx] is not None:
                        if not last_device_states[dev_idx] and is_on: 
                            stats[dev_key]['on_count'] += 1
                            stats[dev_key]['avg_on_hour'].append(hour + (datetime.fromtimestamp(d['ts']).minute / 60.0))
                        elif last_device_states[dev_idx] and not is_on: 
                             stats[dev_key]['avg_off_hour'].append(hour + (datetime.fromtimestamp(d['ts']).minute / 60.0))
                    
                    last_device_states[dev_idx] = is_on

            
            
            heating_rates = []
            session_start = None
            for i, d in enumerate(data_list): 
                if d['kombiDurum'] and session_start is None:
                    session_start = d
                elif (not d['kombiDurum'] or (i > 0 and d['ts'] - data_list[i-1]['ts'] > 1800)) and session_start is not None:
                    duration_seconds = data_list[i-1]['ts'] - session_start['ts']
                    start_temp = session_start['temp']
                    end_temp = data_list[i-1]['temp']
                    if duration_seconds >= 1200 and end_temp > start_temp + 0.3:
                        duration_hours = duration_seconds / 3600
                        rate = (end_temp - start_temp) / duration_hours
                        if 0.5 <= rate <= 5.0: heating_rates.append(rate)
                    session_start = None
            
            avg_heating_rate = 0
            if heating_rates:
                avg_heating_rate = sum(heating_rates) / len(heating_rates)
                avg_heating_rate = round(avg_heating_rate, 2)
                logger.info(f"🔥 {user_id}: Ortalama ısınma hızı: {avg_heating_rate} °C/Saat")

            
            
            
            peak_hours_indices = sorted(range(len(stats['kombi']['peak_hours'])), key=lambda i: stats['kombi']['peak_hours'][i], reverse=True)[:3]
            
            final_stats = {
                'guncelleme': datetime.now(timezone.utc).isoformat(),
                'isinma_hizi': avg_heating_rate if avg_heating_rate > 0 else 1.5,
                'kombi': {
                    'toplam_calisma_saat_7gun': round(stats['kombi']['total_hours'], 1),
                    'gunluk_ort_saat': round(stats['kombi']['total_hours'] / 7.0, 1),
                    'yogun_saatler': peak_hours_indices,
                    'dongu_sayisi': stats['kombi']['cycles']
                }
            }
            
            
            for dev_idx in range(1, 5):
                dev_key = f'device{dev_idx}'
                s = stats[dev_key]
                on_hours = s['avg_on_hour']
                off_hours = s['avg_off_hour']
                
                final_stats[dev_key] = {
                    'toplam_calisma_saat_7gun': round(s['total_hours'], 1),
                    'gunluk_ort_saat': round(s['total_hours'] / 7.0, 1),
                    'ort_acilis': round(sum(on_hours)/len(on_hours), 1) if on_hours else None,
                    'ort_kapanis': round(sum(off_hours)/len(off_hours), 1) if off_hours else None,
                    'acilis_sayisi': s['on_count']
                }

            if kombi_acilis_saatleri:
                en_sik = max(set(kombi_acilis_saatleri), key=kombi_acilis_saatleri.count)
                final_stats['sabah_acilis_saati'] = en_sik
            
            if tercih_sicakliklar:
                final_stats['tercih_sicaklik'] = round(statistics.mean(tercih_sicakliklar), 1)
            
            
            db.reference(f'users/{user_id}/ai/aliskanlik').set(final_stats)
            
            
            logger.info(f"✅ {user_id}: İstatistik analizi tamamlandı.")
            
        except Exception as e:
            logger.error(f"❌ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 15 minutes", memory=512)
def kombi_performans_kontrol(event):
    """
    Kombi açıkken ısınma/soğuma performansını kontrol eder
    """
    logger.info("🔥 Kombi performans kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            kombi_on = termostat.get('kombionoff', False)    
            mevcut = termostat.get('mevcutderece', 20)
            hedef = termostat.get('hedefderece', 23)
            
            
            
            
            
            real_kombi_durum = termostat.get('kombiDurum') 
            
            if real_kombi_durum is not None:
                
                if isinstance(real_kombi_durum, str):
                    kombi_durum = real_kombi_durum.lower() in ('true', '1', 'on', 'aktif')
                else:
                    kombi_durum = bool(real_kombi_durum)
            else:
                
                kombi_durum = kombi_on and (mevcut < hedef)
            
            if not kombi_durum:  
                continue
            
            
            dataset = user_data.get('ai', {}).get('dataset', {})
            otuz_dk_once = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp())
            recent = []
            for d in dataset.values():
                try:
                    ts = int(d.get('sourceTimestamp', '0'))
                    
                    if ts >= otuz_dk_once and d.get('kombiDurum') == 1:
                        recent.append(d)
                except (ValueError, TypeError):
                    continue
            
            if len(recent) < 6:  
                continue
            
            
            if mevcut < hedef - 2:  
                ilk_sicaklik = recent[0].get('currentTemp', mevcut)
                if mevcut <= ilk_sicaklik + 0.5:  
                    gonder_akilli_bildirim(user_id,
                        "⚠️ Ev Isınmıyor",
                        f"Kombi 30 dakikadır açık ama {mevcut:.1f}°C'de kalıyor. Kontrol et.",
                        {
                            'action': 'turn_off'
                        })
                    logger.warning(f"   🚨 {user_id}: Isınma sorunu")
            
            
            elif mevcut > hedef + 2:  
                gonder_akilli_bildirim(user_id,
                    "🔥 Fazla Sıcak",
                    f"Hedef {hedef:.1f}°C ama {mevcut:.1f}°C. Başka ısı kaynağı var mı?",
                    {
                        'action': 'turn_off'
                    })
                logger.warning(f"   🚨 {user_id}: Aşırı ısınma")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 30 minutes")
def evden_uzaklik_kontrol(event):
    """
    Kullanıcı evden uzun süre uzaksa ve kombi açıksa öneride bulunur
    """
    logger.info("🏠 Uzaklık kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    simdi = datetime.now(timezone.utc).timestamp()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            home_away = user_data.get('ai', {}).get('home_away', {})
            if home_away.get('status') != 'AWAY':
                continue
            
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            kombi_on = termostat.get('kombionoff', False)
            
            if not kombi_on:
                continue
            
            
            away_timestamp = home_away.get('timestamp', simdi)
            away_duration = (simdi - away_timestamp) / 3600  
            
            if away_duration >= 2:
                mevcut = termostat.get('mevcutderece', 20)
                yeni_hedef = max(mevcut - 2, 18)  
                
                yeni_hedef = round(yeni_hedef, 1)
                
                
                last_notification = user_data.get('ai', {}).get('last_away_notification', 0)
                if simdi - last_notification > 7200:  
                    gonder_akilli_bildirim(user_id,
                        "💡 Enerji Tasarrufu",
                        f"2 saattir evde değilsin. Kombi kapatılsın mı veya hedef {yeni_hedef:.1f}°C'ye düşürülsün mü?",
                        {
                            'action': 'away_mode',
                            'new_target': str(yeni_hedef)  
                        })
                    
                    db.reference(f'users/{user_id}/ai/last_away_notification').set(int(simdi))
                    logger.info(f"   📲 {user_id}: Uzaklık önerisi")
                    
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 3 hours")
def hava_durumu_kontrol(event):
    """
    Hava soğuyacaksa ve kullanıcı dışarıdaysa, eve geldiğinde sıcak olması için önerir
    """
    logger.info("❄️ Hava durumu kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            home_away = user_data.get('ai', {}).get('home_away', {})
            is_away = home_away.get('status') == 'AWAY'
            
            if not is_away:
                continue
            
            
            home_loc = user_data.get('ai', {}).get('home_location', {})
            lat = home_loc.get('lat', 40.1435)
            lon = home_loc.get('lon', 29.9811)
            
            
            try:
                url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric&lang=tr"
                response = requests.get(url, timeout=10)
                response.raise_for_status()  
                weather = response.json()
                
                
                if 'list' not in weather or len(weather['list']) < 3:
                    logger.warning(f"   ⚠️ {user_id}: Hava durumu verisi yetersiz")
                    continue
                
                
                now_temp = weather['list'][0]['main']['temp']
                future_temp = weather['list'][2]['main']['temp']
                diff = now_temp - future_temp
            except requests.exceptions.RequestException as e:
                logger.error(f"   ❌ {user_id}: Hava durumu API hatası - {str(e)}")
                continue
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"   ❌ {user_id}: Hava durumu veri parse hatası - {str(e)}")
                continue
            
            
            if diff >= 4:
                termostat = user_data.get('Kontrol', {}).get('termostat', {})
                hedef = termostat.get('hedefderece', 23)
                
                if isinstance(hedef, float):
                    hedef = round(hedef, 1)
                yeni_hedef = min(hedef + 2, 26)
                
                yeni_hedef = round(yeni_hedef, 1)
                
                gonder_akilli_bildirim(user_id,
                    "❄️ Hava Soğuyacak",
                    f"6 saatte {diff:.0f}°C soğuma bekleniyor. Eve geldiğinde sıcak olması için hedef {yeni_hedef:.1f}°C yapılsın mı?",
                    {
                        'action': 'weather_heat',
                        'new_target': str(yeni_hedef)  
                    })
                logger.info(f"   📲 {user_id}: Hava uyarısı (+2°C önerisi)")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="every 15 minutes", memory=512)
def kritik_durum_kontrol(event):
    """
    Cihaz offline ve donma riski kontrolü
    """
    logger.info("🚨 Kritik kontrol...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    simdi = datetime.now(timezone.utc).timestamp()
    
    for user_id, user_data in users.items():
        try:
            termostat = user_data.get('Kontrol', {}).get('termostat', {})
            son_guncelleme = termostat.get('son_guncelleme')
            
            
            if son_guncelleme:
                try:
                    if isinstance(son_guncelleme, str):
                        
                        if 'T' in son_guncelleme or 'Z' in son_guncelleme:
                            son_guncelleme_ts = datetime.fromisoformat(son_guncelleme.replace('Z', '+00:00')).timestamp()
                        else:
                            
                            son_guncelleme_ts = float(son_guncelleme)
                    elif isinstance(son_guncelleme, (int, float)):
                        son_guncelleme_ts = float(son_guncelleme)
                    else:
                        continue
                    
                    
                    if simdi - son_guncelleme_ts > 900:
                        
                        gonder_bildirim(user_id,
                            "❌ Cihaz Offline",
                            "15 dakikadır veri gelmiyor. İnternet bağlantısını kontrol et.")
                        logger.warning(f"   🚨 {user_id}: Offline")
                        continue
                    else:
                        
                        
                        
                        pass
                except (ValueError, TypeError, AttributeError) as e:
                    logger.debug(f"   ⚠️ {user_id}: Timestamp parse hatası - {str(e)}")
                    continue
            
            
            sicaklik = termostat.get('mevcutderece', 20)
            if sicaklik < 12:
                
                
                ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
                if ai_settings.get('notify', True):  
                    gonder_akilli_bildirim(user_id,
                        "🥶 DONMA RİSKİ",
                        f"Sıcaklık {sicaklik:.1f}°C'ye düştü! Kombiyi hemen aç.",
                        {
                            'action': 'heat_emergency'
                        })
                    logger.warning(f"   🚨 {user_id}: Donma riski!")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")





@scheduler_fn.on_schedule(schedule="0 9 * * 1")  
def haftalik_rapor_gonder(event):
    """
    Geçen haftanın özetini gönderir
    """
    logger.info("📊 Haftalık rapor...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    gecen_hafta = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-W%U')
    
    for user_id, user_data in users.items():
        try:
            ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
            if not ai_settings.get('notify', False):
                continue
            
            
            haftalik_log = user_data.get('ai', {}).get('haftalik_log', {})
            gecen_hafta_data = haftalik_log.get(gecen_hafta)
            
            if not gecen_hafta_data:
                continue
            
            calisma_saat = gecen_hafta_data.get('toplam_calisma_saat', 0)
            
            
            onceki_hafta = (datetime.now(timezone.utc) - timedelta(days=14)).strftime('%Y-W%U')
            onceki_hafta_data = haftalik_log.get(onceki_hafta)
            
            mesaj = f"📊 Geçen hafta: {calisma_saat}h kombi çalıştı"
            
            if onceki_hafta_data:
                onceki_saat = onceki_hafta_data.get('toplam_calisma_saat', 0)
                if onceki_saat > 0:
                    fark = ((onceki_saat - calisma_saat) / onceki_saat) * 100
                    if abs(fark) > 5:  
                        if fark > 0:
                            mesaj += f"\n🌱 Önceki haftaya göre %{fark:.0f} tasarruf!"
                        else:
                            mesaj += f"\n📈 Önceki haftaya göre %{abs(fark):.0f} fazla kullanım"
            
            gonder_bildirim(user_id, "📊 Haftalık Özet", mesaj)
            logger.info(f"   📊 {user_id}: Rapor gönderildi")
            
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: {str(e)}")



logger = logging.getLogger('cloud_functions')

@scheduler_fn.on_schedule(schedule="*/10 * * * *", memory=1024)
def programi_uygula(event):
    """
    TR Saati ile çalışır. 10 dakikada bir tetiklenir.
    Yeni yapıya göre (target_temp ve devices map) çalışır.
    """
    
    utc_now = datetime.now(timezone.utc)
    tr_now = utc_now + timedelta(hours=3) 
    
    current_day = tr_now.weekday()  
    current_hour = tr_now.hour
    raw_minute = tr_now.minute
    
    current_minute = (raw_minute // 10) * 10 
    
    current_time_minutes = current_hour * 60 + current_minute

    logger.info(f"⏰ Program Kontrolü Başladı - TR Saati: {tr_now.strftime('%Y-%m-%d %H:%M')} (Gün: {current_day})")

    try:
        users_ref = db.reference('users')
        users = users_ref.get()

        if not users:
            logger.info("Kullanıcı bulunamadı.")
            return

        for user_id, user_data in users.items():
            
            program_root = user_data.get('Program')
            if not program_root or not isinstance(program_root, dict):
                continue

            
            home_away_status = user_data.get('ai', {}).get('home_away', {}).get('status', 'UNKNOWN')
            
            
            for prog_id, prog_data in program_root.items():
                
                if prog_id in ['onay_bekleyen', 'weekly', 'location'] or not isinstance(prog_data, dict):
                    continue

                
                if not prog_data.get('enabled', False):
                    continue

                
                
                
                if 'day' in prog_data:
                    
                    if prog_data.get('day') != current_day:
                        continue 

                    open_hour = prog_data.get('open_hour', 0)
                    open_minute = (prog_data.get('open_minute', 0) // 10) * 10
                    close_hour = prog_data.get('close_hour', 0)
                    close_minute = (prog_data.get('close_minute', 0) // 10) * 10
                    
                    start_mins = open_hour * 60 + open_minute
                    end_mins = close_hour * 60 + close_minute
                    
                    start_mins = open_hour * 60 + open_minute
                    end_mins = close_hour * 60 + close_minute

                    
                    
                    minutes_until_start = start_mins - current_time_minutes
                    
                    
                    if 0 < minutes_until_start <= 120 and 'target_temp' in prog_data:
                        
                        isinma_hizi = user_data.get('ai', {}).get('aliskanlik', {}).get('isinma_hizi', 1.5) 
                        mevcut_temp = user_data.get('Kontrol', {}).get('termostat', {}).get('mevcutderece', 20.0)
                        target_temp = float(prog_data['target_temp'])
                        
                        if mevcut_temp < target_temp:
                            
                            temp_diff = target_temp - mevcut_temp
                            if isinma_hizi > 0:
                                needed_hours = temp_diff / isinma_hizi
                                needed_minutes = needed_hours * 60
                                
                                
                                
                                if needed_minutes >= (minutes_until_start - 10):
                                    logger.info(f"🔥 {user_id}: PRE-HEATING Aktif! Program: {prog_id} Hedef: {target_temp}°C Fark: {temp_diff}°C Süre: {int(needed_minutes)}dk")
                                    
                                    
                                    db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                        'hedefderece': target_temp,
                                        'ekomod': False, 
                                        'kombionoff': True
                                    })
                                    
                                    continue
                    if current_hour == open_hour and current_minute == open_minute:
                        logger.info(f"✅ {user_id}: Program BAŞLATILIYOR ({prog_id})")
                        
                        
                        devices = prog_data.get('devices')
                        if devices and isinstance(devices, dict):
                            for dev_name, state in devices.items():
                                
                                db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(state)

                        
                        if 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                            target = float(prog_data['target_temp'])
                            db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                'hedefderece': target,
                                'ekomod': prog_data.get('eko_mode', True),
                                'kombionoff': True
                            })

                    
                    elif current_hour == close_hour and current_minute == close_minute:
                        logger.info(f"🛑 {user_id}: Program SONLANDIRILIYOR ({prog_id})")
                        
                        
                        devices = prog_data.get('devices')
                        if devices and isinstance(devices, dict):
                            for dev_name in devices.keys():
                                
                                db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(False)

                        
                        if 'target_temp' in prog_data:
                            db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                'hedefderece': 23.0, 
                                'kombionoff': True   
                            })

                    
                    
                    elif 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                        in_range = False
                        if start_mins < end_mins:
                            in_range = start_mins <= current_time_minutes < end_mins
                        else: 
                            in_range = current_time_minutes >= start_mins or current_time_minutes < end_mins
                        
                        if in_range:
                            target = float(prog_data['target_temp'])
                            
                            current_val = user_data.get('Kontrol', {}).get('termostat', {}).get('hedefderece')
                            if current_val != target:
                                db.reference(f'users/{user_id}/Kontrol/termostat/hedefderece').set(target)

                
                
                
                elif 'trigger' in prog_data:
                    trigger_type = prog_data.get('trigger') 
                    
                    
                    last_trigger = user_data.get('ai', {}).get('home_away', {}).get('last_program_trigger_time', 0)
                    status_ts = user_data.get('ai', {}).get('home_away', {}).get('timestamp', 0)
                    
                    
                    if status_ts > last_trigger:
                        should_run = False
                        if trigger_type == 'leave' and home_away_status == 'AWAY':
                            should_run = True
                        elif trigger_type == 'arrive' and home_away_status == 'HOME':
                            should_run = True
                            
                        if should_run:
                            logger.info(f"📍 {user_id}: Konum Programı Tetikleniyor ({trigger_type})")
                            
                            
                            devices = prog_data.get('devices')
                            if devices and isinstance(devices, dict):
                                for dev_name, state in devices.items():
                                    
                                    db.reference(f'users/{user_id}/Kontrol/{dev_name}/durum').set(state)
                                    
                            
                            if 'target_temp' in prog_data and prog_data['target_temp'] is not None:
                                target = float(prog_data['target_temp'])
                                db.reference(f'users/{user_id}/Kontrol/termostat').update({
                                    'hedefderece': target,
                                    'ekomod': True,
                                    'kombionoff': True
                                })

                            
                            db.reference(f'users/{user_id}/ai/home_away/last_program_trigger_time').set(int(datetime.now().timestamp()))

    except Exception as e:
        logger.error(f"Program döngüsünde hata: {str(e)}")


logger = logging.getLogger('cloud_functions')

class Config:
    MIN_REQUIRED_DATA = 20
    MIN_RECENT_DATA = 14
    DAYS_TO_ANALYZE = 7
    MIN_PATTERN_COUNT = 2
    PATTERN_CONFIDENCE_THRESHOLD = 0.5
    DEFAULT_OPENING_HOUR = 7
    DEFAULT_TEMP = 23.0
    MIN_TEMP = 18.0
    MAX_TEMP = 30.0
    HOURS_IN_DAY = 24
    DAYS_IN_WEEK = 7






@dataclass
class PatternData:
    kombi_acik_sayisi: int = 0
    kombi_toplam: int = 0
    hedef_sicakliklar: List[float] = None
    cihazlar: Dict[str, int] = None
    
    def __post_init__(self):
        if self.hedef_sicakliklar is None:
            self.hedef_sicakliklar = []
        if self.cihazlar is None:
            self.cihazlar = defaultdict(int)


@dataclass
class GuvenilirPattern:
    kombi_acik: bool = False
    hedef_sicaklik: float = Config.DEFAULT_TEMP
    cihazlar: Set[str] = None
    
    def __post_init__(self):
        if self.cihazlar is None:
            self.cihazlar = set()


@dataclass
class ZamanAraligi:
    baslangic: int
    bitis: int
    kombi_acik: bool
    hedef_sicaklik: float
    cihazlar: List[str]
    
    def to_dict(self) -> Dict:
        
        summary = {
            'baslangic_saat': self.baslangic,
            'bitis_saat': self.bitis,
            'kombi_acik': self.kombi_acik,
        }
        if self.kombi_acik:
            summary['hedef_sicaklik'] = self.hedef_sicaklik
        
        if self.cihazlar:
            
            summary['cihazlar'] = self.cihazlar
            
        return summary






class DataValidator:
   
    @staticmethod
    def parse_timestamp(data: Dict) -> int:
        try:
            ts_str = data.get('sourceTimestamp', '0')
            return int(ts_str) if isinstance(ts_str, (int, str)) else 0
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def validate_hour(hour: int) -> bool:
        return isinstance(hour, int) and 0 <= hour < Config.HOURS_IN_DAY
    
    @staticmethod
    def validate_temp(temp: float) -> bool:
        return temp and Config.MIN_TEMP <= temp <= Config.MAX_TEMP
    
    @staticmethod
    def validate_day(day: int) -> bool:
        return isinstance(day, int) and 0 <= day < Config.DAYS_IN_WEEK


class DatasetAnalyzer:
    
    def __init__(self, dataset: Dict):
        self.dataset = dataset
        self.validator = DataValidator()
    
    def get_recent_data(self, days: int = Config.DAYS_TO_ANALYZE) -> List[Dict]:
        cutoff_timestamp = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
        )
        recent = []
        for data in self.dataset.values():
            ts = self.validator.parse_timestamp(data)
            if ts >= cutoff_timestamp:
                recent.append(data)
        return recent
    
    def analyze_opening_hours(self, recent_data: List[Dict]) -> Optional[int]:
        opening_hours = []
        last_state = None
        sorted_data = sorted(recent_data, key=lambda x: self.validator.parse_timestamp(x))
        
        for data in sorted_data:
            hour = data.get('hour', 0)
            if not self.validator.validate_hour(hour): continue
            kombi_on = data.get('kombiOn', 0) == 1
            if last_state is False and kombi_on:
                opening_hours.append(hour)
            last_state = kombi_on
            
        if not opening_hours: return None
        return max(set(opening_hours), key=opening_hours.count)
    
    def analyze_preferred_temp(self, recent_data: List[Dict]) -> Optional[float]:
        temps = []
        for data in recent_data:
            target = data.get('targetTemp', 0)
            if self.validator.validate_temp(target):
                temps.append(float(target))
        if not temps: return None
        return round(statistics.mean(temps), 1)
    
    def analyze_user_habits(self, aliskanlik: Dict, recent_data: List[Dict]) -> Tuple[int, float]:
        if aliskanlik.get('sabah_acilis_saati'):
            return aliskanlik.get('sabah_acilis_saati'), aliskanlik.get('tercih_sicaklik', Config.DEFAULT_TEMP)
        
        acilis_saati = self.analyze_opening_hours(recent_data) or Config.DEFAULT_OPENING_HOUR
        tercih_sicaklik = self.analyze_preferred_temp(recent_data) or Config.DEFAULT_TEMP
        return acilis_saati, tercih_sicaklik


class PatternAnalyzer:

    def __init__(self, dataset: Dict):
        self.dataset = dataset
        self.validator = DataValidator()
    
    def build_daily_patterns(self, recent_data: List[Dict]) -> Dict[int, Dict[int, PatternData]]:
        patterns = defaultdict(lambda: defaultdict(lambda: PatternData(hedef_sicakliklar=[], cihazlar=defaultdict(int))))
        
        for data in recent_data:
            ts = self.validator.parse_timestamp(data)
            if ts == 0: continue
            
            tarih = datetime.fromtimestamp(ts, tz=timezone.utc)
            gun = tarih.weekday()
            hour = data.get('hour', -1)
            
            if not self.validator.validate_hour(hour): continue
            
            pattern = patterns[gun][hour]
            pattern.kombi_toplam += 1
            if data.get('kombiDurum', 0) == 1:
                pattern.kombi_acik_sayisi += 1
            
            target = data.get('targetTemp', 0)
            if self.validator.validate_temp(target):
                pattern.hedef_sicakliklar.append(float(target))
            
            
            for i in range(1, 5):
                if data.get(f'device{i}', 0) == 1:
                    pattern.cihazlar[f'cihaz{i}'] += 1
        
        return patterns
    
    def extract_reliable_patterns(self, daily_patterns: Dict, default_temp: float) -> Dict:
        reliable = defaultdict(lambda: defaultdict(lambda: GuvenilirPattern(hedef_sicaklik=default_temp, cihazlar=set())))
        
        for gun in range(Config.DAYS_IN_WEEK):
            for saat in range(Config.HOURS_IN_DAY):
                if saat not in daily_patterns[gun]: continue
                pattern = daily_patterns[gun][saat]
                
                if pattern.kombi_toplam >= Config.MIN_PATTERN_COUNT:
                    reliable_pattern = reliable[gun][saat]
                    oran = pattern.kombi_acik_sayisi / pattern.kombi_toplam
                    if oran > Config.PATTERN_CONFIDENCE_THRESHOLD:
                        reliable_pattern.kombi_acik = True
                        if pattern.hedef_sicakliklar:
                            reliable_pattern.hedef_sicaklik = round(statistics.mean(pattern.hedef_sicakliklar), 1)
                    
                    for cihaz, kullanim in pattern.cihazlar.items():
                        if kullanim >= Config.MIN_PATTERN_COUNT:
                            reliable_pattern.cihazlar.add(cihaz)
        return reliable


class TimeIntervalBuilder:
   
    @staticmethod
    def create_intervals(reliable_patterns: Dict[int, GuvenilirPattern]) -> List[ZamanAraligi]:
        intervals = []
        baslangic_saat = None
        onceki_ozellik = None
        
        for saat in range(Config.HOURS_IN_DAY):
            
            if saat not in reliable_patterns:
                if baslangic_saat is not None:
                    intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                    baslangic_saat = None
                    onceki_ozellik = None
                continue
            
            pattern = reliable_patterns[saat]
            
            mevcut_ozellik = (
                pattern.kombi_acik,
                pattern.hedef_sicaklik,
                tuple(sorted(pattern.cihazlar))
            )
            
            
            if not pattern.kombi_acik and not pattern.cihazlar:
                if baslangic_saat is not None:
                    intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                    baslangic_saat = None
                    onceki_ozellik = None
                continue

            if onceki_ozellik is None:
                baslangic_saat = saat
                onceki_ozellik = mevcut_ozellik
            elif mevcut_ozellik != onceki_ozellik:
                
                intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, saat, onceki_ozellik))
                baslangic_saat = saat
                onceki_ozellik = mevcut_ozellik
        
        if baslangic_saat is not None:
            intervals.append(TimeIntervalBuilder._create_interval(baslangic_saat, Config.HOURS_IN_DAY, onceki_ozellik))
            
        return intervals
    
    @staticmethod
    def _create_interval(baslangic: int, bitis: int, ozellik: Tuple) -> ZamanAraligi:
        return ZamanAraligi(
            baslangic=baslangic,
            bitis=bitis,
            kombi_acik=ozellik[0],
            hedef_sicaklik=ozellik[1],
            cihazlar=list(ozellik[2])
        )


class ProgramGenerator:
    
    def __init__(self, user_id: str):
        self.user_id = user_id
    
    def generate_programs(self, intervals_by_day: Dict[int, List[ZamanAraligi]], default_temp: float, 
                         include_temp: bool = True, allowed_devices: List[str] = None) -> Tuple[Dict, List[Dict]]:
        
        program_updates = {}
        interval_summaries = []
        
        for day, intervals in intervals_by_day.items():
            if not intervals: continue
            
            
            
            
            
            filtered_intervals = []
            for interval in intervals:
                
                if not include_temp:
                    interval.kombi_acik = False
                    interval.hedef_sicaklik = 0
                
                
                if interval.cihazlar and allowed_devices is not None:
                    
                    
                    filtered_devices = [d for d in interval.cihazlar if d in allowed_devices]
                    interval.cihazlar = filtered_devices
                
                
                if not interval.kombi_acik and not interval.cihazlar:
                    continue
                    
                filtered_intervals.append(interval)

            if not filtered_intervals:
                continue

            
            merged_intervals = self._merge_similar_intervals(filtered_intervals, max_gap=1)
            
            logger.info(f"📅 Gün {day}: {len(merged_intervals)} program oluşturuluyor (Filtreli)")

            for item in merged_intervals:
                
                program_data = {
                    'day': day,
                    'open_hour': item['start'],
                    'open_minute': 0,
                    'close_hour': item['end'],
                    'close_minute': 0,
                    'enabled': False,
                    'source': 'ai',
                }
                
                
                if item['kombi_acik']:
                    program_data['target_temp'] = item['temp']
                    program_data['eko_mode'] = True
                else:
                    program_data['target_temp'] = None
                    program_data['eko_mode'] = None
                    
                
                if item['devices']:
                    program_data['devices'] = item['devices']
                else:
                    program_data['devices'] = None
                    
                
                if program_data['target_temp'] is None and program_data['devices'] is None:
                    continue

                program_ref = db.reference(f'users/{self.user_id}/Program').push()
                program_id = program_ref.key
                
                if program_id:
                    program_updates[f'Program/{program_id}'] = program_data
                    
                    
                    log_msg = f"✅ Prog: {item['start']}-{item['end']}"
                    if item['kombi_acik']: log_msg += f" | 🌡️ {item['temp']}°C"
                    if item['devices']: log_msg += f" | 🔌 {list(item['devices'].keys())}"
                    logger.info(log_msg)

            
            for interval in filtered_intervals:
                interval_summaries.append(interval.to_dict())

        
        
        
        
        if include_temp:
            location_ref = db.reference(f'users/{self.user_id}/Program').push()
            location_id = location_ref.key
            if location_id:
                location_program = {
                    'trigger': 'arrive',
                    'target_temp': default_temp,
                    'enabled': False,
                    'source': 'ai',
                    'devices': None 
                }
                program_updates[f'Program/{location_id}'] = location_program
        
        return program_updates, interval_summaries

    def _merge_similar_intervals(self, intervals: List[ZamanAraligi], max_gap: int = 1) -> List[Dict]:
        if not intervals: return []
        
        sorted_intervals = sorted(intervals, key=lambda x: x.baslangic)
        merged = []
        
        
        first = sorted_intervals[0]
        current = {
            'start': first.baslangic,
            'end': first.bitis,
            'kombi_acik': first.kombi_acik,
            'temp': first.hedef_sicaklik,
            'devices': {dev: True for dev in first.cihazlar} if first.cihazlar else {} 
        }
        
        for interval in sorted_intervals[1:]:
            
            
            i_kombi = interval.kombi_acik
            i_temp = interval.hedef_sicaklik
            i_devices = {dev: True for dev in interval.cihazlar} if interval.cihazlar else {}
            
            gap = interval.baslangic - current['end']
            
            is_same_config = (
                current['kombi_acik'] == i_kombi and
                abs(current['temp'] - i_temp) < 0.5 and 
                set(current['devices'].keys()) == set(i_devices.keys()) 
            )
            
            if gap <= max_gap and is_same_config:
                current['end'] = max(current['end'], interval.bitis)
            else:
                merged.append(current)
                
                current = {
                    'start': interval.baslangic,
                    'end': interval.bitis,
                    'kombi_acik': i_kombi,
                    'temp': i_temp,
                    'devices': i_devices
                }
        
        merged.append(current)
        return merged






@https_fn.on_call(
    cors=options.CorsOptions(cors_origins="*", cors_methods=["get", "post"]),
    memory=options.MemoryOption.MB_512,
    timeout_sec=120
)
def program_olustur(req: https_fn.CallableRequest) -> Dict:
    if not req.auth:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Kimlik doğrulama gerekli')
    
    user_id = req.auth.uid
    
    
    include_temp = req.data.get('include_temp', True)
    allowed_devices = req.data.get('allowed_devices') 
    
    
    try:
        logger.info(f"🔄 Program oluşturma başladı: {user_id} | Filtre: Temp={include_temp}, Devs={allowed_devices}")
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.NOT_FOUND, message='Kullanıcı verisi bulunamadı')
        
        dataset = user_data.get('ai', {}).get('dataset', {})
        aliskanlik = user_data.get('ai', {}).get('aliskanlik', {})
        
        
        if not dataset or len(dataset) < Config.MIN_REQUIRED_DATA:
            return {'success': False, 'message': f'Yeterli veri yok ({len(dataset) if dataset else 0}/{Config.MIN_REQUIRED_DATA})'}
        
        
        analyzer = DatasetAnalyzer(dataset)
        recent_data = analyzer.get_recent_data()
        
        if len(recent_data) < Config.MIN_RECENT_DATA:
            return {'success': False, 'message': f'Son {Config.DAYS_TO_ANALYZE} günde yeterli veri yok.'}
        
        acilis_saati, tercih_sicaklik = analyzer.analyze_user_habits(aliskanlik, recent_data)
        
        pattern_analyzer = PatternAnalyzer(dataset)
        daily_patterns = pattern_analyzer.build_daily_patterns(recent_data)
        reliable_patterns = pattern_analyzer.extract_reliable_patterns(daily_patterns, tercih_sicaklik)
        
        intervals_by_day = {}
        for day in range(Config.DAYS_IN_WEEK):
            intervals = TimeIntervalBuilder.create_intervals(reliable_patterns[day])
            if intervals: intervals_by_day[day] = intervals
        
        
        program_gen = ProgramGenerator(user_id)
        program_updates, interval_summaries = program_gen.generate_programs(
            intervals_by_day, 
            tercih_sicaklik,
            include_temp=include_temp,
            allowed_devices=allowed_devices
        )
        
        if program_updates:
             
            db.reference(f'users/{user_id}').update(program_updates)
            
            
            onay_bekleyen_data = {
                'aktif': False,
                'olusturma_zamani': datetime.now(timezone.utc).isoformat(),
                'acilis_saati': acilis_saati,
                'tercih_sicaklik': tercih_sicaklik,
                'program_count': len(program_updates),
                'araliklar': interval_summaries
            }
            db.reference(f'users/{user_id}/Program/onay_bekleyen').set(onay_bekleyen_data)
            
            return {
                'success': True,
                'message': 'Program oluşturuldu.',
                'program_count': len(program_updates),
                'generated_programs': True
            }
        else:
            return {'success': False, 'message': 'Kriterlere uygun güvenilir pattern bulunamadı.'} 
    except Exception as e:
        logger.error(f"Hata: {str(e)}")
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))






@https_fn.on_call(cors=options.CorsOptions(cors_origins="*", cors_methods=["post"]), memory=options.MemoryOption.MB_256)
def program_onayla(req: https_fn.CallableRequest) -> Dict:
    if not req.auth: raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Auth required')
    user_id = req.auth.uid
    try:
        
        onay_ref = db.reference(f'users/{user_id}/Program/onay_bekleyen')
        if not onay_ref.get(): return {'success': False, 'message': 'Onay bekleyen yok'}
        
        
        program_ref = db.reference(f'users/{user_id}/Program')
        all_programs = program_ref.get()
        updates = {}
        count = 0
        
        if all_programs:
            for pid, pdata in all_programs.items():
                if isinstance(pdata, dict) and pdata.get('source') == 'ai':
                    
                    if not pdata.get('enabled'):
                        updates[f'Program/{pid}/enabled'] = True
                        count += 1
        
        updates['Program/onay_bekleyen/aktif'] = True
        db.reference(f'users/{user_id}').update(updates)
        return {'success': True, 'message': f'{count} program aktif edildi', 'activated_count': count}
    except Exception as e:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))

@https_fn.on_call(cors=options.CorsOptions(cors_origins="*", cors_methods=["post"]), memory=options.MemoryOption.MB_256)
def program_reddet(req: https_fn.CallableRequest) -> Dict:
    if not req.auth: raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.UNAUTHENTICATED, message='Auth required')
    user_id = req.auth.uid
    try:
        program_ref = db.reference(f'users/{user_id}/Program')
        all_programs = program_ref.get()
        count = 0
        if all_programs:
            for pid, pdata in all_programs.items():
                
                
                if isinstance(pdata, dict) and pdata.get('source') == 'ai' and not pdata.get('enabled', False):
                    db.reference(f'users/{user_id}/Program/{pid}').delete()
                    count += 1
        
        db.reference(f'users/{user_id}/Program/onay_bekleyen').delete()
        return {'success': True, 'message': f'{count} taslak program silindi'}
    except Exception as e:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.INTERNAL, message=str(e))



@scheduler_fn.on_schedule(schedule="0 4 * * *")  
def ai_dataset_gunluk_temizlik(event):
    logger.info("🧹 AI Dataset günlük temizlik...")
    
    users_ref = db.reference('users')
    users = users_ref.get()
    
    yirmi_bir_gun_once = int((datetime.now(timezone.utc) - timedelta(days=21)).timestamp())
    toplam_silinen = 0
    
    for user_id, user_data in users.items():
        try:
            dataset = user_data.get('ai', {}).get('dataset', {})
            if not dataset:
                continue
            
            silinen = 0
            for key, data in list(dataset.items()):
                try:
                    
                    ts_str = data.get('sourceTimestamp', '0')
                    if isinstance(ts_str, str):
                        ts = int(ts_str)
                    else:
                        ts = int(ts_str)
                    
                    
                    if ts < yirmi_bir_gun_once:
                        db.reference(f'users/{user_id}/ai/dataset/{key}').delete()
                        silinen += 1
                except (ValueError, TypeError) as e:
                    logger.debug(f"   ⚠️ {user_id}: Dataset kayıt parse hatası - {key}: {str(e)}")
                    continue
            
            if silinen > 0:
                toplam_silinen += silinen
                logger.info(f"   🗑️ {user_id}: {silinen} dataset kaydı silindi")
                
        except Exception as e:
            logger.error(f"   ⏭️ {user_id}: Dataset temizlik hatası - {str(e)}")
    
    if toplam_silinen > 0:
        logger.info(f"✅ Toplam {toplam_silinen} dataset kaydı temizlendi (21 günden eski)")
    else:
        logger.info("✅ Temizlenecek eski kayıt bulunamadı")





def gonder_bildirim(user_id, baslik, mesaj):
    try:
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            logger.warning(f"   ⚠️ {user_id}: Kullanıcı bulunamadı")
            return False
        
        fcm_token = user_data.get('fcm_token')
        
        if not fcm_token:
            logger.debug(f"   ⚠️ {user_id}: FCM token yok")
            return False
        
        
        ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
        if not ai_settings.get('notify', True):
            logger.debug(f"   ⚠️ {user_id}: Bildirimler kapalı")
            return False
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=baslik,
                body=mesaj,
            ),
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority='high',
                notification=messaging.AndroidNotification(
                    sound='default'
                )
            )
        )
        
        response = messaging.send(message)
        logger.info(f"   📲 {user_id}: Bildirim gönderildi - {response}")
        return True
        
    except messaging.UnregisteredError:
        logger.warning(f"   ⚠️ {user_id}: FCM token geçersiz, token siliniyor")
        
        try:
            db.reference(f'users/{user_id}/fcm_token').delete()
        except:
            pass
        return False
    except messaging.SenderIdMismatchError:
        logger.error(f"   ❌ {user_id}: FCM sender ID uyuşmazlığı")
        return False
    except Exception as e:
        logger.error(f"   ❌ {user_id}: Bildirim hatası - {str(e)}")
        return False


def gonder_akilli_bildirim(user_id, baslik, mesaj, data):
    
    try:
        user_ref = db.reference(f'users/{user_id}')
        user_data = user_ref.get()
        
        if not user_data:
            logger.warning(f"   ⚠️ {user_id}: Kullanıcı bulunamadı")
            return False
        
        fcm_token = user_data.get('fcm_token')
        
        if not fcm_token:
            logger.debug(f"   ⚠️ {user_id}: FCM token yok")
            return False
        
        
        ai_settings = user_data.get('Kontrol', {}).get('ai_setting', {})
        if not ai_settings.get('notify', True):
            logger.debug(f"   ⚠️ {user_id}: Bildirimler kapalı")
            return False
        
        
        
        data_str = {str(k): str(v) for k, v in data.items()}
        data_str['title'] = baslik
        data_str['body'] = mesaj
        data_str['type'] = 'action_notification'  
        
        
        
        message = messaging.Message(
            data=data_str,  
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority='high'
            )
        )
        
        response = messaging.send(message)
        logger.info(f"   📲 {user_id}: Akıllı bildirim gönderildi - {response}")
        return True
        
    except messaging.UnregisteredError:
        logger.warning(f"   ⚠️ {user_id}: FCM token geçersiz, token siliniyor")
        try:
            db.reference(f'users/{user_id}/fcm_token').delete()
        except:
            pass
        return False
    except messaging.SenderIdMismatchError:
        logger.error(f"   ❌ {user_id}: FCM sender ID uyuşmazlığı")
        return False
    except Exception as e:
        logger.error(f"   ❌ {user_id}: Akıllı bildirim hatası - {str(e)}")
        return False
>>>>>>> 2c4889efa6ef6da213f432eb60fe86a755546eda
