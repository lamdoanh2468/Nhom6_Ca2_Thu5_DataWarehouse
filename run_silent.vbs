Set WshShell = CreateObject("WScript.Shell")
' Số 0 ở cuối nghĩa là ẩn cửa sổ đi
WshShell.Run chr(34) & "D:\LaptopDW\run_etl.bat" & chr(34), 0
Set WshShell = Nothing