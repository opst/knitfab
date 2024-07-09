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
            serviceAccount: "{{ .Values.nurse.serviceAccount }}"
            image: "{{ .Values.imageRepository }}{{ ternary "" "/" (empty .Values.imageRepository) }}{{ .Values.nurse.image }}:{{ .Chart.AppVersion }}"
