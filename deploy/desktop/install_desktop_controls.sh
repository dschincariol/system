#!/usr/bin/env bash
# install_desktop_controls.sh — install the desktop Start/Stop controls for the
# trading system on this workstation. Run as the desktop user 'david' (who must
# be able to sudo). Idempotent.
#
# It installs:
#   /opt/trading/bin/trading-ctl                       (root:root 0755)
#   /opt/trading/bin/trading-cred-show                 (root:root 0755)
#   /etc/polkit-1/rules.d/49-trading-system.rules      (root:root 0644)
#   ~/.local/share/applications/trading-{start,stop,token}.desktop  (david)
#   ~/Desktop/trading-{start,stop,token}.desktop  (symlinks, KDE-trusted)
#
# It does NOT enable LAN exposure, the firewall, or remove blanket sudo — those
# are deliberate, separately-reviewed steps (see deploy/desktop/README.md).
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER_NAME="${SUDO_USER:-$USER}"
USER_HOME="$(getent passwd "$USER_NAME" | cut -d: -f6)"
APPS_DIR="$USER_HOME/.local/share/applications"
DESKTOP_DIR="$USER_HOME/Desktop"

echo "[install] target user=$USER_NAME home=$USER_HOME"

# --- root-owned binaries + polkit rule ---
sudo install -D -o root -g root -m 0755 "$SRC/trading-ctl"       /opt/trading/bin/trading-ctl
sudo install -D -o root -g root -m 0755 "$SRC/trading-cred-show" /opt/trading/bin/trading-cred-show
sudo install -D -o root -g root -m 0644 "$SRC/49-trading-system.rules" /etc/polkit-1/rules.d/49-trading-system.rules
echo "[install] binaries + polkit rule placed (polkit hot-reloads rules.d; no daemon reload needed)"

# --- desktop entries (as the user) ---
install -d "$APPS_DIR" "$DESKTOP_DIR"
for f in trading-start trading-stop trading-token; do
  install -m 0644 "$SRC/$f.desktop" "$APPS_DIR/$f.desktop"
  ln -sfn "$APPS_DIR/$f.desktop" "$DESKTOP_DIR/$f.desktop"
  chmod +x "$DESKTOP_DIR/$f.desktop" 2>/dev/null || true
  # mark trusted so KDE/Plasma does not show the "untrusted application" prompt
  gio set "$DESKTOP_DIR/$f.desktop" metadata::trusted true 2>/dev/null || true
done
update-desktop-database "$APPS_DIR" 2>/dev/null || true
echo "[install] desktop launchers placed in $DESKTOP_DIR and $APPS_DIR"

echo "[install] done. Verify with: /opt/trading/bin/trading-ctl status"
