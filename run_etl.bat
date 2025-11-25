@echo off
<<<<<<< HEAD
cd /d D:\LaptopDW\
=======
cd /d D:\LaptopDW
>>>>>>> 37e85c8e437488d3877e4c3075e20262db63a72b

:: Không cần set utf-8 nữa vì Python lo rồi, nhưng để cũng không sao
set PYTHONIOENCODING=utf-8

:: Gọi Python chạy file Scheduler (Không cần >> log nữa)
"D:\LaptopDW\venv\Scripts\python.exe" "scripts\Scheduler.py"