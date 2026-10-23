#!/usr/bin/env bash
# Autostart for the touch app (mesh-screen.py) on the Pi desktop. No sudo needed: run as the Pi user.
#   bash ~/mesh-screen-autostart.sh
# The desktop's xdg-autostart runs mesh-screen-run.sh, which restarts the app if it crashes
# (closing it from the power menu exits with code 0 and stops the loop).
set -euo pipefail

mkdir -p ~/.config/autostart
cat > ~/.config/autostart/mesh-screen.desktop <<EOF
[Desktop Entry]
Type=Application
Name=Mesh screen
Exec=$HOME/mesh-screen-run.sh
X-GNOME-Autostart-enabled=true
EOF

cat > ~/mesh-screen-run.sh <<EOF
#!/bin/sh
export DISPLAY=:0
sleep 4
while true; do
  $HOME/mesh-bridge-venv/bin/python $HOME/mesh-screen.py >> /tmp/mesh-screen.log 2>&1
  [ \$? -eq 0 ] && break
  sleep 3
done
EOF
chmod +x ~/mesh-screen-run.sh
sync
echo "ok: ~/.config/autostart/mesh-screen.desktop, ~/mesh-screen-run.sh"
