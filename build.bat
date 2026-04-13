@echo off
setlocal
chcp 65001 >nul

cd /d "%~dp0"
set "BUILD_VENV=%CD%\.build-venv-win"
set "BUILD_PYTHON=%BUILD_VENV%\Scripts\python.exe"
set "DIST_FOLDER=%CD%\dist\抖音罗盘抓取器"
set "RELEASE_FOLDER=%CD%\dist\抖音罗盘抓取器_Windows版"
set "ZIP_PATH=%CD%\dist\douyin-compass-scraper-windows.zip"

echo ============================================
echo   抖音罗盘数据抓取器 - Windows 打包脚本
echo ============================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到 Python，请先安装 Python 3.10+
    pause
    exit /b 1
)

echo [1/5] 创建干净的打包虚拟环境...
if exist "%BUILD_VENV%" rmdir /s /q "%BUILD_VENV%"
if exist "%CD%\ms-playwright" rmdir /s /q "%CD%\ms-playwright"
python -m venv "%BUILD_VENV%"
if errorlevel 1 (
    echo [错误] 虚拟环境创建失败
    pause
    exit /b 1
)

echo.
echo [2/5] 在虚拟环境中安装 Python 依赖...
"%BUILD_PYTHON%" -m pip install -r requirements.txt
if errorlevel 1 (
    echo [错误] 依赖安装失败
    pause
    exit /b 1
)

echo.
echo [3/5] 预装 Chromium 到 Playwright 包内...
set "PLAYWRIGHT_BROWSERS_PATH=0"
"%BUILD_PYTHON%" "%CD%\seed_playwright_browsers.py"
if errorlevel 1 (
    echo [INFO] 本机缓存不可用，改为在线安装 Chromium...
    "%BUILD_PYTHON%" -m playwright install chromium
    if errorlevel 1 (
        echo [错误] Chromium 预装失败
        pause
        exit /b 1
    )
)

echo.
echo [4/5] 使用虚拟环境中的 PyInstaller 生成桌面应用...
"%BUILD_PYTHON%" -m PyInstaller --clean --noconfirm douyin_compass.spec
if errorlevel 1 (
    echo [错误] 打包失败，请查看上方错误信息
    pause
    exit /b 1
)

echo.
echo [5/5] 整理新手友好的 Windows 便携版并生成压缩包...
"%BUILD_PYTHON%" "%CD%\build_release_assets.py" windows
if errorlevel 1 (
    echo [错误] Windows 发布目录整理失败
    pause
    exit /b 1
)

if exist "%ZIP_PATH%" del /f /q "%ZIP_PATH%"
powershell -NoProfile -ExecutionPolicy Bypass -Command "Compress-Archive -Path '%RELEASE_FOLDER%' -DestinationPath '%ZIP_PATH%' -Force"
if errorlevel 1 (
    echo [错误] ZIP 压缩包生成失败
    pause
    exit /b 1
)

echo.
echo ============================================
echo   打包完成!
echo   Windows 分发包: dist\douyin-compass-scraper-windows.zip
echo   解压后双击: dist\抖音罗盘抓取器_Windows版\双击启动-抖音罗盘抓取器.bat
echo   打包虚拟环境: %BUILD_VENV%
echo.
echo   当前产物已内置 Chromium：
echo   1. 首次运行无需联网安装浏览器
echo   2. Windows 下不再弹出安装用的空白 CMD 窗口
echo   3. 新手只需要解压后双击启动脚本
echo ============================================
echo.
pause
