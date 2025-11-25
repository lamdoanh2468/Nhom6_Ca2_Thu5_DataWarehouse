@echo off
cd /d D:\LaptopDW\

:: Không cần set utf-8 nữa vì Python lo rồi, nhưng để cũng không sao
set PYTHONIOENCODING=utf-8

:: Gọi Python chạy file Scheduler (Không cần >> log nữa)
"D:\LaptopDW\venv\Scripts\python.exe" "scripts\Scheduler.py"