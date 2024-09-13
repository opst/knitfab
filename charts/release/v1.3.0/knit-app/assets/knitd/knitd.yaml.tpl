dburi: "postgres://{{ .Values.database.service }}/knit"
backendapiroot: http://{{ .Values.knitd_backend.service }}
serverport: 8080
