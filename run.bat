@echo off
REM ============================================================
REM  Postcodes Pipeline Runner (Windows)
REM  - Builds GeoAreaLookup.csv from Register files
REM  - Builds PostcodeLookup.csv from NSPL
REM ============================================================

REM Change directory to the folder this file lives in
cd /d %~dp0

echo.
echo === Step 1: Building geographic lookups ===
python build_geo_lookups.py
IF %ERRORLEVEL% NEQ 0 goto :error

echo.
echo === Step 2: Building postcode lookup ===
python build_postcode_lookup.py
IF %ERRORLEVEL% NEQ 0 goto :error

echo.
echo All done! Outputs are in the .\out folder.
pause
exit /b 0

:error
echo.
echo ERROR: Something went wrong.
echo Please screenshot this window and share it with support.
pause
exit /b 1
