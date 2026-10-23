#!/usr/bin/env bash
# Let the Pi user shut down / reboot from the touch app without a password prompt.
# Run on the Pi:  sudo bash ~/mesh-power-install.sh
set -euo pipefail

USER_NAME=${SUDO_USER:-chechen}
FILE=/etc/sudoers.d/mesh-power
TMP=$(mktemp)
cat > "$TMP" <<EOF
$USER_NAME ALL=(root) NOPASSWD: /usr/bin/systemctl poweroff, /usr/bin/systemctl reboot
EOF
visudo -cf "$TMP"
install -m 440 "$TMP" "$FILE"
rm -f "$TMP"
sync
echo "ok: $FILE"
sudo -n -l -U "$USER_NAME" | grep -E 'poweroff|reboot' || true
