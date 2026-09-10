![MarmotteDB](docs/images/github_title_with_mascotte.png)

# MarmotteDB

[English version](README.md)

> [!WARNING]
> MarmotteDB n’est pas terminé. Il s’agit d’un POC (proof of concept) en cours
> de développement, qui n’est pas encore prêt pour une utilisation en production.

## Algorithmes d’indexation

MarmotteDB sert notamment à expérimenter et comparer plusieurs algorithmes
d’indexation. Différentes approches seront implémentées et testées au fil du
développement afin d’évaluer leurs performances, leur consommation de ressources
et leurs compromis selon les cas d’usage.

### Sorted Index Table

![Schéma de la Sorted Index Table](docs/images/sorted-index-table.fr.png)

La Sorted Index Table est un index trié par fragments actuellement en cours
d’expérimentation. Son format disque, son API et ses limites de persistance sont
présentés dans la [documentation dédiée](docs/sorted-index-table.fr.md).

Exécuter les tests depuis la racine du dépôt :

```powershell
cargo test --offline --manifest-path marmotte-server/Cargo.toml
```
