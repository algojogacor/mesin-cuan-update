@echo off
chcp 65001 >nul
cd /d D:\upgrade-mesin-cuan
call venv\Scripts\activate

:MENU
cls
echo.
echo ╔══════════════════════════════════════════════════════════╗
echo ║          🤖  MESIN CUAN  —  Auto Content Machine         ║
echo ╠══════════════════════════════════════════════════════════╣
echo ║  PIPELINE UTAMA                                          ║
echo ║  [1]  🚀  Render Semua Channel  (Campaign Mode)          ║
echo ║  [2]  🎯  Render 1 Channel Saja                          ║
echo ║  [3]  🧪  Dry Run  (Test tanpa upload)                   ║
echo ║  [4]  ⚡  Render Semua + Skip QC Vision                  ║
echo ╠══════════════════════════════════════════════════════════╣
echo ║  MONITORING                                              ║
echo ║  [5]  📅  Preview Slot Campaign                          ║
echo ║  [6]  📊  Update Analytics Retention Harian              ║
echo ║  [7]  🔄  Sync Video Terbaru ke DB  (dari YouTube)       ║
echo ╠══════════════════════════════════════════════════════════╣
echo ║  MODE LAIN                                               ║
echo ║  [8]  🎬  Legacy Mode  (daily_plan settings.json)        ║
echo ║  [9]  🗂️  Buka Folder Logs                               ║
echo ║  [10] 🧹  Cleanup File Lama Sekarang                     ║
echo ╠══════════════════════════════════════════════════════════╣
echo ║  [0]  ❌  Keluar                                         ║
echo ╚══════════════════════════════════════════════════════════╝
echo.
set /p pilihan="  Masukkan pilihan: "

if "%pilihan%"=="1"  goto RENDER_ALL
if "%pilihan%"=="2"  goto RENDER_ONE
if "%pilihan%"=="3"  goto DRY_RUN
if "%pilihan%"=="4"  goto RENDER_SKIP_QC
if "%pilihan%"=="5"  goto PREVIEW
if "%pilihan%"=="6"  goto ANALYTICS
if "%pilihan%"=="7"  goto SYNC_DB
if "%pilihan%"=="8"  goto LEGACY
if "%pilihan%"=="9"  goto OPEN_LOGS
if "%pilihan%"=="10" goto CLEANUP
if "%pilihan%"=="0"  goto EXIT

echo  ⚠️  Pilihan tidak valid. Coba lagi.
timeout /t 2 >nul
goto MENU

:: ─────────────────────────────────────────────────────────────────────────────
:RENDER_ALL
cls
echo  🚀 Render semua channel (Campaign Mode)...
echo.
python main.py
goto DONE

:RENDER_ONE
cls
echo  Channel yang tersedia (lihat config/settings.json):
echo   ch_id_horror / ch_id_psych / ch_en_horror / ch_en_psych
echo.
set /p ch="  Masukkan Channel ID: "
echo  🎯 Render channel: %ch%
echo.
python main.py --channel %ch%
goto DONE

:DRY_RUN
cls
echo  🧪 Dry Run — tidak ada upload ke GDrive
echo.
python main.py --dry-run
goto DONE

:RENDER_SKIP_QC
cls
echo  ⚡ Render semua channel + Skip QC Vision (lebih cepat)
echo.
python main.py --skip-qc
goto DONE

:PREVIEW
cls
echo  📅 Preview status slot campaign...
echo.
python main.py --preview
goto DONE

:ANALYTICS
cls
echo  📊 Update analytics retention harian dari YouTube...
echo.
python main.py --analytics
goto DONE

:SYNC_DB
cls
echo  🔄 Sync video terbaru ke database lokal...
echo      (browser mungkin terbuka untuk login OAuth2 pertama kali)
echo.
python -c "
from engine.utils import load_settings
from engine.retention_engine import sync_recent_videos
settings = load_settings()
for ch in settings.get('channels', []):
    if ch.get('active'):
        print(f'Syncing {ch[\"id\"]}...')
        n = sync_recent_videos(ch['id'])
        print(f'  -> {n} video synced')
print('Selesai!')
"
goto DONE

:LEGACY
cls
echo.
echo  Mode legacy: semua channel / 1 channel saja?
echo  [1] Semua channel
echo  [2] 1 channel saja
echo.
set /p lg="  Pilihan: "
if "%lg%"=="1" (
    python main.py --legacy
) else if "%lg%"=="2" (
    set /p lch="  Masukkan Channel ID: "
    python main.py --legacy --channel %lch%
) else (
    echo  Pilihan tidak valid.
)
goto DONE

:OPEN_LOGS
cls
echo  🗂️  Membuka folder logs...
if exist "logs" (
    start explorer logs
) else (
    echo  Folder logs belum ada.
)
goto DONE

:CLEANUP
cls
echo  🧹 Cleanup file audio/footage/temp lama sekarang...
echo.
python -c "
from engine import cleanup_engine
cleanup_engine.run(dry_run=False)
print('Cleanup selesai!')
"
goto DONE

:DONE
echo.
echo ──────────────────────────────────────────────────────────
set /p lagi="  Kembali ke menu? (y/n): "
if /i "%lagi%"=="y" goto MENU

:EXIT
echo.
echo  Sampai jumpa! 👋
echo.
