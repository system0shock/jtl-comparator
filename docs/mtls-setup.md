# mTLS setup

Краткая инструкция по запуску JTL Comparator с HTTPS/mTLS.

## 1. Сертификаты

Если банк уже выдал серверный сертификат, ключ и CA для клиентских сертификатов, переходите сразу к шагу 3.

Если нужны тестовые сертификаты:

```bash
./scripts/gen-certs.sh <hostname>
```

Пример:

```bash
./scripts/gen-certs.sh localhost
```

Будут созданы файлы в `certs/`:

- `ca.crt`, `ca.key`
- `server.crt`, `server.key`, `server.csr`
- `client.crt`, `client.key`, `client.p12`

## 2. Дополнительные клиентские сертификаты

Один сертификат на инженера:

```bash
./scripts/gen-client-cert.sh ivan.petrov
```

Скрипт создаст `certs/ivan.petrov.p12` для импорта в браузер.

## 3. Запуск сервера

Рекомендуемый вариант: указать пути сертификатов в INI-конфиге.

Создайте файл `config/mtls.ini` (или любой другой и передайте через `TLS_CONFIG`):

```ini
[mtls]
tls_cert = /etc/jtl-comparator/certs/server.crt
tls_key = /etc/jtl-comparator/certs/server.key
tls_ca = /etc/jtl-comparator/certs/ca.crt
```

Относительные пути в `TLS_CONFIG` считаются относительно директории самого config-файла.

Запуск с конфигом:

```bash
TLS_CONFIG=/etc/jtl-comparator/mtls.ini python app.py
```

Минимальный запуск mTLS:

```bash
TLS_CERT=/etc/jtl-comparator/certs/server.crt \
TLS_KEY=/etc/jtl-comparator/certs/server.key \
TLS_CA=/etc/jtl-comparator/certs/ca.crt \
python app.py
```

Или через helper-скрипт (по умолчанию ожидает `TLS_CONFIG=/etc/jtl-comparator/mtls.ini`):

```bash
./scripts/start-mtls.sh
```

## 4. Импорт клиентского сертификата в браузер

Импортируйте `.p12` в профиль инженера в Chrome/Firefox и открывайте сервис по `https://<host>:<port>`.

## 5. Проверка

С клиентским сертификатом:

```bash
curl --cert certs/client.crt --key certs/client.key --cacert certs/ca.crt https://localhost:8443/
```

Без клиентского сертификата (должен быть отклонен при mTLS):

```bash
curl --cacert certs/ca.crt https://localhost:8443/
```
