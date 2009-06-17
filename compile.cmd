@echo off
..\bjam toolset=msvc %*

IF NOT ERRORLEVEL 1 (
    call ..\boost-version.cmd
    for /r %BUILD_DIRECTORY% %%f in (*.pdb) do xcopy /D /Y %%f ..\dist\bin
)

IF NOT ERRORLEVEL 1 (
    ..\dist\bin\fost-schema-test-dyndriver -b false ado fost-ado.dll
)

IF NOT ERRORLEVEL 1 (
    ..\dist\bin\ftest -b false -i Configuration/ado.ini ..\dist\bin\fost-ado-test-smoke.dll
)
