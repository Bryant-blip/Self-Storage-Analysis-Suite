@echo off
set APP_DIR=%~dp0

powershell -NoProfile -Command "$desktop = [Environment]::GetFolderPath('Desktop'); $ws = New-Object -ComObject WScript.Shell; $sc = $ws.CreateShortcut($desktop + '\Storage Comps App.lnk'); $sc.TargetPath = 'pythonw'; $sc.Arguments = '\"%APP_DIR%storage_comps_app.py\"'; $sc.WorkingDirectory = '%APP_DIR%'; $sc.Description = 'Self Storage Market Rent Comps'; $sc.Save(); Write-Host ('Shortcut created at: ' + $desktop)"

echo.
pause
