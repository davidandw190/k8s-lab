#!/bin/sh
# this should be ran in the Vault pod

cat > /tmp/vault-setup.sh << 'EOF'
#!/bin/sh

export VAULT_TOKEN="root"
export VAULT_ADDR="http://localhost:8200"

vault secrets enable -path=minio kv-v2

vault policy write minio-policy - << 'POLICY'
path "minio/data/*" {
  capabilities = ["read"]
}
POLICY

vault auth enable kubernetes

vault write auth/kubernetes/config \
    kubernetes_host="https://kubernetes.default.svc" \
    token_reviewer_jwt="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
    kubernetes_ca_cert="$(cat /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)" \
    issuer="https://kubernetes.default.svc.cluster.local"

vault write auth/kubernetes/role/minio-role \
    bound_service_account_names=minio-vault-auth \
    bound_service_account_namespaces=minio-system \
    policies=minio-policy \
    ttl=1h

vault kv put minio/data/config \
    MINIO_ROOT_USER="<OWN_CREDENTIALS>" \
    MINIO_ROOT_PASSWORD="<OWN_CREDENTIALS>" \

echo "Vault setup completed successfully!"
EOF

chmod +x /tmp/vault-setup.sh
/tmp/vault-setup.sh