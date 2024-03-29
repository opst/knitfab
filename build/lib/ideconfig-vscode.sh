#! /bin/bash

echo "---" >&2
echo "Add items below into 'configurations' of your '.vscode/launch.json' ." >&2
echo ""
echo " - Sorry, this is NOT AUTOMATIC. Update your config on your own." >&2
echo " - There are placeholders surrounded '< ... >', fill it along with your environment." >&2
echo "===" >&2
echo "" >&2

cat <<EOF
[
    {
        "name": "Attach: knitd",
        "type": "go",
        "request": "attach",
        "mode": "remote",
        "backend": "native",
        "debugAdapter": "dlv-dap",
        "substitutePath": [
            {"to": "/work", "from": "${ROOT}"},
            {"to": "/go", "from": "$(go env GOPATH)"},
            {"to": "/usr/local/go", "from": "$(go env GOROOT)"},
        ],
        "port": 30990,
        "host": "<YOUR K8S NODE IP>",
        "showLog": true,
        "apiVersion": 2,
    },
    {
        "name": "Attach: knitd backend",
        "type": "go",
        "request": "attach",
        "mode": "remote",
        "backend": "native",
        "debugAdapter": "dlv-dap",
        "substitutePath": [
            {"to": "/work", "from": "${ROOT}"},
            {"to": "/go", "from": "$(go env GOPATH)"},
            {"to": "/usr/local/go", "from": "$(go env GOROOT)"},
        ],
        "port": 30991,
        "host": "<YOUR K8S NODE IP>",
        "showLog": true,
        "apiVersion": 2,
    }
]
EOF

echo "" >&2
echo "---" >&2
echo "" >&2