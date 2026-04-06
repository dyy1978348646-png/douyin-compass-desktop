#!/usr/bin/env bash
# ============================================
#   抖音罗盘数据抓取器 - macOS 打包脚本
#   输出 .app 和可拖拽安装的 .dmg
# ============================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_NAME="抖音罗盘抓取器"
DIST_DIR="${SCRIPT_DIR}/dist"
APP_PATH="${DIST_DIR}/${APP_NAME}.app"
DMG_PATH="${DIST_DIR}/${APP_NAME}.dmg"
BUILD_VENV_DIR="${SCRIPT_DIR}/.build-venv-macos"
BUILD_PYTHON="${BUILD_VENV_DIR}/bin/python3"
STAGING_DIR=""

cleanup() {
    if [ -n "${STAGING_DIR}" ] && [ -d "${STAGING_DIR}" ]; then
        rm -rf "${STAGING_DIR}"
    fi
}
trap cleanup EXIT

create_dmg() {
    local app_path="$1"
    local dmg_path="$2"

    if [ ! -d "${app_path}" ]; then
        echo "[错误] 未找到 .app 产物: ${app_path}"
        exit 1
    fi

    STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/douyin-compass-dmg.XXXXXX")"
    mkdir -p "${STAGING_DIR}"

    ditto "${app_path}" "${STAGING_DIR}/${APP_NAME}.app"
    ln -s /Applications "${STAGING_DIR}/Applications"

    rm -f "${dmg_path}"
    hdiutil create \
        -volname "${APP_NAME}" \
        -srcfolder "${STAGING_DIR}" \
        -ov \
        -format UDZO \
        "${dmg_path}" >/dev/null
}

echo "============================================"
echo "  ${APP_NAME} - macOS 打包"
echo "  架构: $(uname -m)"
echo "============================================"
echo ""

if ! command -v python3 &>/dev/null; then
    echo "[错误] 未检测到 python3，请先安装："
    echo "  brew install python@3.11 python-tk@3.11"
    exit 1
fi

if ! command -v hdiutil &>/dev/null; then
    echo "[错误] 未检测到 hdiutil，无法创建 .dmg"
    exit 1
fi

PYTHON_ARCH=$(python3 -c "import platform; print(platform.machine())")
echo "[INFO] Python 架构: ${PYTHON_ARCH}"

if [ "$(uname -m)" = "arm64" ] && [ "${PYTHON_ARCH}" != "arm64" ]; then
    echo "[警告] 当前 Python 是 x86_64 版本（通过 Rosetta 运行）"
    echo "       建议安装 arm64 原生 Python 以获得最佳性能："
    echo "       brew install python@3.11"
fi

if ! python3 -c "import tkinter" 2>/dev/null; then
    echo "[错误] tkinter 未安装。请执行："
    echo "  brew install python-tk@3.11"
    echo "  （版本号需与你的 Python 版本匹配）"
    exit 1
fi

cd "${SCRIPT_DIR}"

echo ""
echo "[1/5] 创建干净的打包虚拟环境..."
rm -rf "${BUILD_VENV_DIR}"
rm -rf "${SCRIPT_DIR}/ms-playwright"
python3 -m venv "${BUILD_VENV_DIR}"

echo ""
echo "[2/5] 在虚拟环境中安装 Python 依赖..."
"${BUILD_PYTHON}" -m pip install -r "${SCRIPT_DIR}/requirements.txt"

echo ""
echo "[3/5] 预装 Chromium 到 Playwright 包内..."
if "${BUILD_PYTHON}" "${SCRIPT_DIR}/seed_playwright_browsers.py"; then
    echo "[INFO] 已从本机 Playwright 缓存注入 Chromium。"
else
    echo "[INFO] 本机缓存不可用，改为在线安装 Chromium。"
    PLAYWRIGHT_BROWSERS_PATH=0 "${BUILD_PYTHON}" -m playwright install chromium
fi

echo ""
echo "[4/5] 使用虚拟环境中的 PyInstaller 打包为 .app..."
"${BUILD_PYTHON}" -m PyInstaller --clean --noconfirm "${SCRIPT_DIR}/douyin_compass.spec"

PLIST="${APP_PATH}/Contents/Info.plist"
if [ -f "${PLIST}" ]; then
    /usr/libexec/PlistBuddy -c "Add :NSHighResolutionCapable bool true" "${PLIST}" 2>/dev/null || true
    /usr/libexec/PlistBuddy -c "Set :CFBundleDisplayName ${APP_NAME}" "${PLIST}" 2>/dev/null || \
    /usr/libexec/PlistBuddy -c "Add :CFBundleDisplayName string ${APP_NAME}" "${PLIST}" 2>/dev/null || true
fi

echo ""
echo "[5/5] 创建可拖拽安装的 .dmg..."
create_dmg "${APP_PATH}" "${DMG_PATH}"

echo ""
echo "============================================"
echo "  打包完成!"
echo "  安装镜像: ${DMG_PATH}"
echo "  调试用 .app: ${APP_PATH}"
echo "  打包虚拟环境: ${BUILD_VENV_DIR}"
echo ""
echo "  用户安装方式："
echo "    1. 双击打开 ${APP_NAME}.dmg"
echo "    2. 将 ${APP_NAME}.app 拖到 Applications"
echo "    3. 从 Applications 启动应用"
echo ""
echo "  当前产物已内置 Chromium，首启无需额外下载。"
echo "  如果 macOS 提示无法打开（未签名），请执行:"
echo "    xattr -cr \"${APP_PATH}\""
echo "============================================"
