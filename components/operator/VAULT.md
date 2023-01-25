# Create a vault for local development

More information : https://developer.hashicorp.com/vault/tutorials/secrets-management/pki-engine

Start the vault server :
```
vault server -dev -dev-root-token-id root
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=root
```

Create the CA :
```
vault secrets enable pki
vault secrets tune -max-lease-ttl=87600h pki
vault write -field=certificate pki/root/generate/internal \
     common_name="formance.com" \
     issuer_name="root" \
     ttl=87600h > ca.crt
vault write pki/roles/temporal allow_any_name=true allow_subdomains=true
vault write pki/config/urls \
     issuing_certificates="$VAULT_ADDR/v1/pki/ca" \
     crl_distribution_points="$VAULT_ADDR/v1/pki/crl"
```

Create the temporal certificate :
```
vault write pki/issue/temporal common_name="*.formance.com" ttl="87599h"
```

Add the CA certificate on temporal and update garden/example* files with certificate and key.
