@echo off
..\bjam toolset=msvc %*

IF NOT ERRORLEVEL 1 (
    ..\dist\bin\fost-schema-test-dyndriver -b false ado fost-ado.dll
    IF NOT ERRORLEVEL 1 (
        ..\dist\bin\fost-schema-test-dyndriver -b false ado.f3 fost-ado.dll
        IF NOT ERRORLEVEL 1 (
            ..\dist\bin\ftest -b false -i Configuration/ado.ini ..\dist\bin\fost-ado-test-smoke.dll
        )
    )
)
