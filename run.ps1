$ErrorActionPreference = "Stop"

$srcMain = "src/main/java"
$srcTest = "src/test/java"
$out = "out"

Write-Host "=== Compiling ==="
New-Item -ItemType Directory -Force $out | Out-Null
$files = Get-ChildItem -Recurse -Filter *.java $srcMain, $srcTest | ForEach-Object { $_.FullName }
javac --release 17 -d $out $files

Write-Host ""
Write-Host "=== Running Tests ==="
java -cp $out rx.RxTests

Write-Host ""
Write-Host "=== Running Demo ==="
java -cp $out demo.Demo
