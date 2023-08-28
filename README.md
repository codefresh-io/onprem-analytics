# Onprem Analytics Tool

1. npm install
2. npm run summary
3. npm run list

## builds summary report (npm run summary)
this will generate a summary report regarding an aggregation of the builds<br>

possible environment variables:<br>
<b>MONGO_URI</b>(required) - mongo db endpoint including authentication if reuqired (default: mongodb://localhost)<br>
<b>API_DB</b>(required) - mongo db api database (default: local) <br>
<b>FROM_DATE</b> - in format of 2019-01-01 (default: 2019-01-01) <br>
<b>TO_DATE</b> - in format of 2019-01-01 (default: not passed => till latest) <br>
<b>ACCOUNT_ID</b> - generate report only for a specific account id. mongo id string. if using ACCOUNT_ID, SYSTEM_OVERVIEW doesn't have an affect<br>
<b>SYSTEM_OVERVIEW</b> - export entire system instead of per account report (default: not passed). if using ACCOUNT_ID, SYSTEM_OVERVIEW doesn't have an affect<br>

## builds list report (npm run list)
This will generate a table list of all builds<br>

possible environment variables:<br>
<b>MONGO_URI</b>(required) - mongo db endpoint including authentication if reuqired (default: mongodb://localhost) <br>
<b>API_DB</b>(required) - mongo db api database (default: local) <br>
<b>FROM_DATE</b> - in format of 2019-01-01 (default: 2019-01-01) <br>
<b>TO_DATE</b> - in format of 2019-01-01 (default: not passed => till latest) <br>
<b>ACCOUNT_ID</b> - generate report only for a specific account id. mongo id string <br>


## TLS

Set as env variables; applicable to both scripts above.

<b>TLS</b> — Enables or disables TLS/SSL for the connection — `"true"`/`"false"` (default: `"false"`)<br>
<b>TLS_CA_FILE</b> — Location of a local .pem file that contains the root certificate chain from the Certificate Authority.<br>
<b>TLS_CERT_FILE</b> — Location of a local TLS Certificate.<br>
<b>TLS_CERT_KEY_FILE</b> — Location of a local .pem file that contains either the client's TLS/SSL certificate and key or only the client's TLS/SSL key when `TLS_CERT_FILE` is used to provide the certificate.<br>
<b>TLS_CERT_KEY_FILE_PASSWORD</b> — Password to de-crypt the `TLS_CERT_KEY_FILE`.<br>
