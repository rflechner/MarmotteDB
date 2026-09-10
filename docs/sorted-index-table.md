# Index trié par fragments

L’implémentation se trouve dans `marmotte-server/src/indexes/sorted_index_table.rs`.
Elle conserve le principe initial : des fichiers `.ix` contenant chacun un index
trié, avec une capacité maximale, une limite de déplacement à l’insertion et une
compaction des fragments incomplets.

## Utilisation dans le serveur

```rust
use crate::indexes::sorted_index_table::{
    default_u32_reader, default_u32_writer, FenseIndex, SortedIndexFiles,
};

fn example() -> Result<(), String> {
    let mut index = SortedIndexFiles::new(
        "indexes/demo/age".to_owned(),
        0u32,                  // bornes d’un fragment vide
        default_u32_reader(),
        default_u32_writer(),
        10,                    // limite de fragments incomplets
        128,                   // limite de références décalées à l’insertion
        1024,                  // capacité de chaque fragment
    )?;

    index.insert(FenseIndex::from_value(4096, 25))?;
    index.insert(FenseIndex::from_value(8192, 30))?;
    let matches = index.find(&25)?;
    assert_eq!(matches[0].target, 4096);
    let interval = index.range(&20, &30)?; // bornes inclusives
    assert_eq!(interval.len(), 2);        // dans un dossier initialement vide
    index.flush()?;
    Ok(())
}
```

`target` est une référence opaque vers les données, choisie par l’appelant. Le
module ne lit pas les documents. Les codecs fournis prennent en charge `String`,
`u32` et `u64`. D’autres types peuvent implémenter `Ord + Clone + BinarySizeable`
avec un lecteur et un écrivain compatibles.

`FenseIndex::from_value` calcule la taille automatiquement et crée une entrée
active. Le constructeur historique `new(target, value, size)` reste disponible ;
une taille incorrecte est refusée à l’écriture. La taille est toujours en octets,
notamment pour les chaînes UTF-8. Les clés vides, zéro et la valeur par défaut sont
des clés ordinaires. Les doublons sont conservés, même lorsque leur couple
`(value, target)` est identique.

Le constructeur ouvre et vérifie tous les fragments existants. La capacité et
le seuil de déplacement doivent correspondre aux valeurs enregistrées. Un dossier
appartient à un seul propriétaire : ne pas l’ouvrir depuis plusieurs instances
ou processus simultanément.

## Insertion et organisation

1. Parcourir les en-têtes en mémoire. Dans chaque fragment ayant de la place,
   chercher par dichotomie la position dans l’ordre `(value, target)`.
2. Si le nombre de références à décaler ne dépasse pas `shift_threshold`, ajouter
   les octets de la valeur en fin de fichier et mettre à jour le suffixe de la
   table de positions. Les anciennes valeurs ne sont pas réécrites.
3. Si aucun fragment disponible ne convient, un fragment plein dont les bornes
   encadrent strictement la valeur peut être scindé. Conformément à la tentative
   initiale, les valeurs strictement supérieures vont dans un nouveau fragment.
   Les autres restent dans l’ancien avec la nouvelle entrée.
4. Sinon, créer un fragment. Les clés égales aux bornes d’un fragment plein sont
   acceptées dans un autre fragment sans provoquer de dépassement.
5. Lorsque le nombre de fragments non vides et non pleins dépasse
   `max_incomplete_fragments_count`, les regrouper, les trier et les répartir en
   fragments remplis au maximum. Les fragments déjà pleins sont exclus de cette
   compaction automatique.

Le seuil peut valoir zéro : seules les insertions en fin de fragment évitent la
création d’un fragment ou une scission. La capacité et la limite de fragments
incomplets doivent être positives.

Les numéros des fichiers indiquent leur ordre de création, pas un ordre global
des valeurs. Les plages peuvent se chevaucher. `find` et `range` consultent tous
les fragments dont les bornes correspondent, puis ordonnent les résultats.
`all` retourne également une vue globalement triée.

## Accès aux fragments et compaction

- `fragment_count()` et `read_header(num)` exposent les métadonnées.
- `read_fragment(num)` lit les entrées actives dans l’ordre des emplacements.
- `read_offset(num, offset)` lit un emplacement logique, ou retourne `None`.
- `write_offset(num, entry, offset)` remplace un emplacement et peut laisser le
  fragment désordonné. Remplacer une entrée n’incrémente pas le compteur.
- `clear_offset(num, offset)` invalide l’emplacement, actualise les bornes et peut
  être appelé plusieurs fois sans diminuer plusieurs fois le compteur.
- `reorder_indexes(num, prefix, start)` trie les références et élimine les trous.
  Dans ce format, `prefix` vaut `FenseIndex::<T>::get_prefix_binary_size()` et
  `start` vaut `read_header(num)?.compute_binary_size() as u64`.
- `compact()` regroupe toutes les entrées et récupère l’espace des anciennes
  valeurs inutiles. Les fichiers vides en fin de série sont supprimés ; les
  fichiers vides intermédiaires sont réutilisables.
- `flush()` appelle `sync_all` sur les fichiers ouverts.

Les emplacements logiques ne sont pas des identifiants stables : tri, insertion,
scission et compaction peuvent les changer. Après une modification bas niveau qui
laisse des trous ou modifie l’ordre, la prochaine insertion ou recherche concernée
trie le fragment avant d’utiliser la dichotomie. Les bornes sont actualisées dès
la modification.

## Format disque MRMTIX02

Tous les entiers sont en big-endian. Les fichiers se nomment `00000000.ix`,
`00000001.ix`, etc., sans numéro manquant.

L’en-tête occupe **24 octets** :

| Position | Taille | Contenu |
| --- | --- | --- |
| 0 | 8 | Signature ASCII `MRMTIX02` |
| 8 | 4 | Capacité en entrées (`u32`) |
| 12 | 4 | Nombre d’entrées actives (`u32`) |
| 16 | 4 | Seuil de déplacement (`u32`) |
| 20 | 4 | Références compactes et triées : 0 ou 1 (`u32`) |

L’en-tête est suivi de `capacité × 21` octets réservés aux emplacements :

| Position dans l’emplacement | Taille | Contenu |
| --- | --- | --- |
| 0 | 1 | Actif : 0 ou 1 |
| 1 | 8 | Cible (`u64`) |
| 9 | 8 | Position absolue de la valeur dans le fichier (`u64`) |
| 17 | 4 | Taille de la valeur en octets (`u32`) |

Un emplacement inactif contient uniquement des zéros. Les valeurs commencent
après cette table. Les chaînes sont en UTF-8 sans préfixe supplémentaire :
leur longueur figure dans l’emplacement. Les nombres utilisent exactement 4 ou 8
octets. Les valeurs minimale et maximale sont reconstruites à l’ouverture et
mises en cache ; elles n’occupent plus un en-tête de taille variable.

Les anciens fichiers expérimentaux sont refusés sans être modifiés. Ils doivent
être reconstruits à partir des données originales ; il n’y a pas de migration
automatique d’un format dont les positions pouvaient déjà être incohérentes.

À l’ouverture, le module vérifie la signature, la configuration, les compteurs,
les indicateurs, les positions et tailles des valeurs, et l’ordre lorsque le
fragment est marqué trié. Une donnée tronquée ou un UTF-8 invalide produit une
erreur. Ces contrôles ne remplacent pas une somme de contrôle des données.

## Performances et limites de cette étape

Les positions et les bornes restent en mémoire ; les clés sont lues sur disque
à la demande. L’ouverture parcourt la table et les clés pour les vérifier. Une
recherche dans un fragment trié utilise la dichotomie puis lit les résultats
consécutifs. La sélection des fragments parcourt encore leurs métadonnées en
mémoire. Une insertion écrit la nouvelle valeur et le suffixe de références
concerné, mais reconstruit encore le vecteur des références en mémoire.

La scission matérialise le fragment concerné. Une compaction automatique
matérialise les fragments incomplets concernés. `all` et la compaction manuelle
peuvent matérialiser tout l’index en mémoire. Les scissions autour de la valeur
insérée peuvent produire des fragments déséquilibrés. Cette version établit une
base fonctionnelle ; elle ne prétend pas fournir un débit ou une latence mesurés
pour une charge de production.

Les écritures ordinaires ne font pas un `fsync` par insertion. Les fichiers
temporaires de compaction sont entièrement écrits et synchronisés avant de
remplacer les originaux ; une erreur d’encodage pendant leur préparation conserve
les originaux. **Les opérations sur plusieurs fichiers ne sont pas atomiques.**
Une coupure ou une erreur d’E/S pendant la publication d’une scission ou d’une
compaction peut laisser une opération partielle, notamment des doublons. Les
écritures de références et d’en-têtes ne sont pas non plus transactionnelles.
Après une erreur d’E/S, ne pas continuer à utiliser l’instance comme si
l’opération avait été annulée. Un journal de récupération et la coordination des
écritures concurrentes restent des travaux distincts.

## Validation

```powershell
cargo test --offline --manifest-path marmotte-server/Cargo.toml
```

Les tests couvrent les cas d’origine, les chaînes variables et Unicode, les trous
et remplacements, les doublons et bornes, les scissions, le seuil de déplacement,
les recherches avec plages qui se chevauchent, les compactions et les réouvertures.
Des séquences déterministes d’opérations pseudo-aléatoires sont comparées à un
modèle indépendant en mémoire. Des fichiers volontairement tronqués ou mal formés
vérifient les erreurs de lecture. Les répertoires de test sont uniques et isolés
dans le répertoire temporaire du système.
