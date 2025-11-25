@echo off
cd /d D:\LaptopDW

:: Không cần set utf-8 nữa vì Python đã lo
set PYTHONIOENCODING=utf-8

:: Gọi Python chạy file Scheduler.py
"D:\LaptopDW\venv\Scripts\python.exe" "scripts\Scheduler.py"