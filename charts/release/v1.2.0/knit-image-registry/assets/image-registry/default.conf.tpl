log_format proxy '<PROXY> $remote_addr - $remote_user [$time_local] '
                '$scheme "$request" ( $status ) <- $proxy_host ( $upstream_status ) '
                '$bytes_sent "$http_referer" "$http_user_agent" "$http_x_forwarded_for"';

map $upstream_http_docker_distribution_api_version $docker_distribution_api_version {
    '' 'registry/2.0';
}

server {
    listen              80{{ if and .Values.certs.cert .Values.certs.key }} ssl{{ end }};

    # 497 comes from https://nginx.org/en/docs/http/ngx_http_ssl_module.html
    error_page 497 301 =307 https://$host:${EXTERNAL_PORT}$request_uri;

    {{ if and .Values.certs.cert .Values.certs.key }}
    ssl_certificate     ${TLS_CERT};
    ssl_certificate_key ${TLS_KEY};
    ssl_protocols       TLSv1.1 TLSv1.2;
    ssl_prefer_server_ciphers on;
    ssl_session_cache shared:SSL:10m;
    ssl_ciphers         'EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH';
    {{ end }}

    access_log /var/log/nginx/access.log  proxy;

    chunked_transfer_encoding on;
    client_max_body_size 0;

    location / {
        proxy_pass http://localhost:8080;

        proxy_set_header  Host              $http_host;
        proxy_set_header  X-Real-IP         $remote_addr;
        proxy_set_header  X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header  X-Forwarded-Proto $scheme;

        add_header 'Docker-Distribution-Api-Version' $docker_distribution_api_version always;
    }
}
