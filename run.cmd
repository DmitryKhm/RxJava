@echo off
setlocal

set SRC_MAIN=src\main\java
set SRC_TEST=src\test\java
set OUT=out

echo === Compiling ===
if not exist "%OUT%" mkdir "%OUT%"

dir /s /b "%SRC_MAIN%\*.java" "%SRC_TEST%\*.java" > "%OUT%\sources.txt"
javac --release 17 -d "%OUT%" @"%OUT%\sources.txt"
if errorlevel 1 exit /b 1

echo.
echo === Running Tests ===
java -cp "%OUT%" rx.RxTests
if errorlevel 1 exit /b 1

echo.
echo === Running Demo ===
java -cp "%OUT%" demo.Demo
