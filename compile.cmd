@echo off
echo fost-windows
..\bjam toolset=msvc %*

IF NOT ERRORLEVEL 1 (
    ..\dist\bin\fost-schema-test-dyndriver -b false ado fost-ado.dll
    IF NOT ERRORLEVEL 1 (
        ..\dist\bin\fost-schema-test-dyndriver -b false ado.f3 fost-ado.dll
    )
)
