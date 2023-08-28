//@ts-check

const tls = process.env.TLS === 'true';
const tlsCAFile = process.env.TLS_CA_FILE || undefined;
const tlsCertificateFile = process.env.TLS_CERT_FILE || undefined;
const tlsCertificateKeyFile = process.env.TLS_CERT_KEY_FILE || undefined;
const tlsCertificateKeyFilePassword = process.env.TLS_CERT_KEY_FILE_PASSWORD || undefined;

/**
 * @type { import('mongodb').MongoClientOptions }
 */
const options = {
  tls,
  tlsCAFile,
  tlsCertificateFile,
  tlsCertificateKeyFile,
  tlsCertificateKeyFilePassword,
};

exports.options = options;
