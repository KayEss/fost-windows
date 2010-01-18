@echo off
echo fost-windows
pushd %0\..

..\bjam preserve-test-targets=on %*

IF ERRORLEVEL 1 (
    popd
    copy
) ELSE (
    popd
)
