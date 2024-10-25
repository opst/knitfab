port: 8080
cluster:
    domain: {{ .Values.clusterTLD }}
    namespace: {{ .Release.Namespace | quote }}
    database: "postgres://{{ .Values.database.service }}/knit"
    dataAgent:
        image: "{{ .Values.imageRepository }}{{ ternary "" "/" (empty .Values.imageRepository) }}{{ .Values.dataagt.image }}:{{ .Chart.AppVersion }}"
        port: {{ .Values.dataagt.port }}
        volume:
            storageClassName: "{{ .Values.storage.class.data }}"
            initialCapacity: "{{ .Values.vex.margin }}"
    worker:
        priority: "{{ .Values.worker.priorityClassName }}"
        init:
            image: "{{ .Values.imageRepository }}{{ ternary "" "/" (empty .Values.imageRepository) }}{{ .Values.empty.image }}:{{ .Chart.AppVersion }}"
        nurse:
            serviceAccountSecret: "{{ .Values.nurse.serviceAccount }}-secret"
            image: "{{ .Values.imageRepository }}{{ ternary "" "/" (empty .Values.imageRepository) }}{{ .Values.nurse.image }}:{{ .Chart.AppVersion }}"
    keychains:
        signKeyForImportToken:
            name: "{{ .Values.knitd_backend.component }}-signer-for-import-token"
