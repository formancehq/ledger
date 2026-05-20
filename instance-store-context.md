# Context: Instance Store NVMe for Ledger Bench

## Objectif

Monter les instance stores NVMe locaux du r8g.8xlarge comme volume `data` pour le pod ledger, afin de bencher avec une latence disque ~100µs au lieu de ~3ms (gp3/io2 EBS).

## Cluster & Node

- **Cluster** : `eks-acme-dev-euw1-01` (eu-west-1)
- **Node** : `ip-10-100-104-13.eu-west-1.compute.internal`
- **Instance type** : `r8g.8xlarge` (Graviton, Nitro)
- **Instance stores disponibles** : 2× 600 GiB NVMe (inclus dans l'instance, pas de coût supplémentaire)
- **Namespace** : `ledger-v3`
- **Pod** : `acme-world-to-bank-res-0-0` (StatefulSet `acme-world-to-bank-res-0`)
- **Node label de scheduling** : `formance.com/dedicated=ledger`

## Volumes actuels du pod

| Mount path | PVC | Usage | Remplacer par instance store ? |
|------------|-----|-------|-------------------------------|
| `/data/app` | `data-acme-world-to-bank-res-0-0` (gp3, 500 GiB) | Pebble data + checkpoints | **Oui** — c'est le bottleneck I/O |
| `/data/raft` | `wal-acme-world-to-bank-res-0-0` (gp3, 15 GiB) | Raft WAL | Non — faible I/O, pas un bottleneck |
| `/data/cold-cache` | `cold-cache-acme-world-to-bank-res-0-0` (gp3, 10 GiB) | Cold cache | Non — idle pendant le bench |

## Ce qu'il faut faire

1. **Sur le node** : formater et monter un des NVMe instance stores (ex: `/dev/nvme1n1`) sur un path comme `/mnt/instance-store`. Filesystem ext4 ou xfs, `noatime,discard`.

2. **Dans le StatefulSet** : remplacer le PVC `data` par un `hostPath` pointant sur le mount :
   ```yaml
   volumes:
     - name: data
       hostPath:
         path: /mnt/instance-store
         type: DirectoryOrCreate
   ```
   Ou utiliser un `local` PersistentVolume si l'opérateur le supporte.

3. **Scheduling** : le pod doit tourner sur le node qui a l'instance store monté. Le label `formance.com/dedicated=ledger` est déjà utilisé pour l'affinité.

## Contraintes

- Les instance stores sont **éphémères** : données perdues si l'instance est stoppée/terminée. C'est acceptable pour un bench — Raft fournit la durabilité via réplication.
- Le node est en Graviton (ARM64) — pas d'impact sur le montage.
- Le disque doit être reformaté à chaque démarrage d'instance (les instance stores ne persistent pas).

## Métriques actuelles (gp3)

- **Queue length** : ~3.0 (saturé par la latence ~3ms/IO)
- **IOPS** : ~1,000 / 4,000 provisionnés (25%)
- **Throughput** : ~250 MB/s / 1,000 MB/s (25%)
- Le disque est sous-utilisé en capacité mais la **latence par I/O** est le vrai bottleneck
- Avec instance store NVMe (~100µs latence), on s'attend à queue length < 0.5 et un gain significatif de TPS
