# MinIO Setup with Vault-Managed Secrets

This is a brief overview explaining how to deploy this MinIO configuration in a K8s cluster with secret management handled by HashiCorp Vault. The setup uses the Vault Agent Injector to automatically inject MinIO credentials into pods.

## Setup

### 1. Install the Vault Injector

```bash
helm repo add hashicorp https://helm.releases.hashicorp.com
helm repo update
helm install vault-injector hashicorp/vault \
 --namespace vault \
 --create-namespace \
 --set "global.openshift=false" \
 --set "server.enabled=false" \
 --set "injector.enabled=true" \
 --set "injector.externalVaultAddr=http://vault.minio-system.svc:8200"

```

### 2. Configure Hashicorp Vault

Connect to the vault pod:

```bash

kubectl exec -it deployment/vault -n minio-system -- /bin/sh
```

Inside the pod, create and run the setup script, which you can get from `scritps/vault-setup.sh`:

```bash
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
    MINIO_ROOT_USER="<ADD_CREDENTIALS>" \
    MINIO_ROOT_PASSWORD="<ADD_CREDENTIALS>"
EOF

chmod +x /tmp/vault-setup.sh
/tmp/vault-setup.sh
```

### 3. Verify the Setup

After configuring everything, we can verify the setup works properly:

```bash
# check vault status
kubectl exec -it deployment/vault -n minio-system -- vault status

# verify the secret exists
kubectl exec -it deployment/vault -n minio-system -- vault kv get minio/data/config
```
