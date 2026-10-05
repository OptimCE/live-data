<p align="center">
  <img src="logo.svg" alt="Logo OptimCE" width="160">
</p>

# OptimCE — Données en direct

[![Site web](https://img.shields.io/badge/Site%20web-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Licence](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-43a047.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](README.nl.md)

Les relevés quart-horaires des compteurs communicants, par MQTT.

Aujourd'hui, les données de comptage d'un membre parviennent à OptimCE via le
gestionnaire de réseau de distribution : avec des mois de retard, dans un
fichier mensuel. C'est suffisant pour facturer et inutile pour tout le reste —
une communauté ne peut pas voir si sa production solaire est consommée
localement *maintenant*, et on ne peut pas dire à un membre que faire tourner le
lave-vaisselle dans l'heure qui vient est gratuit. Ce service est l'autre
chemin : un connecteur dans le local compteur qui publie les relevés au fil de
l'eau.

C'est un service **annexe** : il possède sa propre base de données, lit celle du
CRM central en lecture seule, et s'assemble avec le reste de la plateforme dans
le [monorepo](https://github.com/OptimCE/monorepo).

> **Périmètre.** La phase 1 est terminée — les onze étapes : ingestion,
> enrôlement, administration des appareils, projection de propriété,
> agrégations et leur ordonnanceur, API de lecture, surface d'exploitation
> et l'*amorce* de prévision. **Aucune méthode de prévision n'est livrée**,
> délibérément : `forecasting/` est le point d'extension et
> `forecasting/methods_implemented/` est vide. Voir
> [CHANGELOG.md](../CHANGELOG.md).

## Le protocole est un contrat publié et gelé

L'essentiel de ce dépôt peut évoluer librement. Le format sur le fil, non.

`docs/live-data-protocol.md`, dans le monorepo, est **gelé en `v1`**. Trois
connecteurs indépendants l'implémentent depuis l'extérieur de ce dépôt, et un
firmware enregistre sa réponse d'enrôlement en mémoire flash sans jamais la
redemander — il n'existe aucune mise à jour à distance pour un boîtier au fond
d'une cave. Le §7 de ce document définit ce qui peut changer : un nouveau champ
*facultatif* est permis en `v1` ; un nouveau champ obligatoire, ou un sens
différent donné à un champ existant, c'est `v: 2`.

La taxonomie des rejets présente ici et le §4.2 là-bas doivent concorder dans
les deux sens, noms **et** portées. Si vous modifiez l'une, modifiez l'autre.

## Comment un relevé arrive

Un connecteur publie sur deux sujets et ne s'abonne à rien :

```
ce/{community_id}/{device_id}/telemetry    QoS 1
ce/{community_id}/{device_id}/status       QoS 0, retenu, également le Last Will
```

Le flux est unidirectionnel par conception. Il n'y a pas de sujet de commande ni
d'accusé de réception qu'un connecteur puisse lire, ce qui lui permet de tourner
sur du matériel sans voie de retour — et c'est la raison d'être des compteurs de
rejets ci-dessous.

Un message de télémétrie transporte jusqu'à 200 mesures dans un seul tableau :
un appareil resté hors ligne envoie donc son arriéré en une seule publication.
Le stockage est idempotent sur `(appareil, ts)` : **renvoyer une mesure l'écrase**.
C'est cette promesse qui rend QoS 1 suffisant et qui fait de « je ne suis pas sûr
que ce soit arrivé, je renvoie » le comportement correct d'un connecteur plutôt
qu'un risque.

## Les rejets ont une portée

Un message mal formé est rejeté silencieusement, comptabilisé sous un motif et
journalisé. Chaque motif est classé selon ce qu'il écarte :

| Portée | Écarte | Motifs |
|---|---|---|
| message | tout le contenu de la publication | `schema_invalid`, `unknown_field`, `batch_too_large`, `duplicate_ts_in_batch`, `device_unknown`, `device_revoked`, `community_mismatch` |
| mesure | un relevé ; le reste du lot est conservé | `ts_in_future`, `ts_too_old`, `ts_not_aligned`, `negative_energy`, `over_device_ceiling`, `implausible_production` |

Cette distinction est le cœur du dispositif. Une quinzaine de jours d'arriéré
contenant un relevé à l'horloge dérivée doit perdre le relevé, pas la quinzaine.
Les rejets de portée message sont écrits dans `ingest_dead_letter` ; ceux de
portée mesure aboutissent dans `device_last.last_reject_reason`.

Deux situations sont comptées sans jamais être rejetées : un intervalle portant
à la fois un soutirage et une injection (banal par un après-midi nuageux), et une
clé inconnue *à l'intérieur* d'une mesure — le §7 interdit de rejeter
celles-ci, et c'est précisément ce qui permet d'ajouter un champ facultatif sans
danger.

## Enrôlement

Un appareil est créé par un gestionnaire de communauté, qui obtient un code
court. Quelqu'un saisit ce code dans le connecteur, au compteur, une seule fois :

```
POST /live-public/enroll     { "token": "…", "connector": { … } }
-> { "broker": {…}, "credentials": {…}, "topics": {…} }
```

C'est le **seul point d'entrée non authentifié** de la plateforme. Le jeton fait
128 bits issus d'un CSPRNG, en base32 de Crockford, stocké sous forme de
condensat SHA-256 et utilisable une fois. L'alphabet neutralise les confusions à
l'oral — `I` et `L` deviennent `1`, `O` devient `0` — parce que le cas réaliste
est un code dicté au téléphone vers un portail captif, un soir d'hiver.

**Le mot de passe est affiché une seule fois et n'est jamais récupérable.**
OptimCE ne le conserve pas ; le courtier n'en garde qu'un condensat. Un secret
perdu impose un réenrôlement — c'est pourquoi l'index unique sur les compteurs
d'une communauté est partiel : un appareil révoqué libère son EAN.

Un jeton expiré, un jeton déjà consommé et un jeton qui n'a jamais existé
produisent une réponse *identique*. Une réponse distinguable apprendrait à un
attaquant que sa supposition était juste.

## Révocation

Immédiate et côté serveur : le client du courtier est désactivé, le message
`status` retenu est effacé, puis le client est supprimé. Un appareil révoqué est
déconnecté en quelques millisecondes et sa reconnexion refusée. Il n'y a aucune
action côté appareil et aucune notification — du point de vue du connecteur, ses
identifiants cessent simplement de fonctionner.

Effacer le statut retenu est l'étape facile à oublier et coûteuse à oublier : un
`status` qui survit à son appareil est rejoué vers le worker d'ingestion à chaque
reconnexion, si bien que l'alerte « hors ligne » se déclenche à chaque
déploiement jusqu'à ce que l'équipe cesse de la lire.

## Points d'entrée

| Méthode | Chemin | Rôle |
|---|---|---|
| `GET` | `/live/version` | version du build et du schéma |
| `GET` | `/live/devices` | les appareils de la communauté |
| `POST` | `/live/devices` | en créer un — valide l'EAN auprès du CRM |
| `POST` | `/live/devices/{id}/token` | émettre un code d'enrôlement (invalide le précédent s'il est inutilisé) |
| `POST` | `/live/devices/{id}/revoke` | révoquer |
| `GET` | `/live/devices/{id}/diagnostics` | pourquoi un appareil se tait — santé, `diag`, connecteur |
| `GET` | `/live/summary` | le signal courant de la communauté. **Plancher MEMBRE** |
| `GET` | `/live/series` | une série agrégée. **Plancher MEMBRE** |
| `GET` | `/live/settings` | visibilité et k. Renvoie les défauts sans écrire |
| `PUT` | `/live/settings` | les remplacer |
| `GET` | `/live/forecast` | vide **avec une raison nommée** tant qu'aucune méthode n'existe |
| `GET` | `/live/forecast/methods` | le registre, actuellement vide |
| `GET` | `/live/ops/health` | la flotte, et l'âge des agrégations |
| `POST` | `/live-public/enroll` | **public.** Échanger un code contre des identifiants |

### Authentification

Tout ce qui se trouve sous `/live` est authentifié à la passerelle, qui valide
le JWT et transmet `x-user-id`, `x-community-id` et `x-user-orgs`. Le service
fait confiance à ces en-têtes et restreint chaque requête à la communauté de
l'appelant ; l'accès est en outre conditionné à un abonnement actif de la
communauté. Tant que cet abonnement est inactif, le worker ignore aussi la
télémétrie de la communauté (les messages de statut restent traités) et
l'ordonnanceur la saute une fois ses agrégations en attente terminées ; les
appareils et l'historique sont conservés, si bien qu'une réactivation ne demande
aucun nouvel enrôlement.

`/live-public/enroll` fait exception et passe par une entrée de passerelle
distincte, sans validateur. La passerelle ne peut pas retirer les en-têtes de
confiance pour cette seule route : c'est donc le reverse proxy qui les vide
avant que la requête n'arrive — un appareil détient un jeton d'enrôlement, jamais
une session.

## Configuration

| | |
|---|---|
| `CRM_DATABASE_URL` | le CRM central, en lecture seule |
| `LOCAL_DATABASE_URL` | la base de données propre au service |
| `SUBSCRIPTION_CACHE_TTL_SECONDS` | combien de temps le worker et l'ordonnanceur se fient à leur copie de la liste des communautés abonnées (60 par défaut, 1–3600) |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_TLS` | le courtier que **ce service appelle** |
| `MQTT_ADMIN_USERNAME` / `_PASSWORD` | la connexion de contrôle dynamic-security |
| `MQTT_INGEST_USERNAME` / `_PASSWORD` | l'identité d'abonné du worker |
| `BROKER_PUBLIC_HOST`, `_PORT`, `_TLS` | l'adresse **communiquée à un appareil** |

Les deux dernières lignes désignent deux choses différentes et doivent le
rester. Un appareil écrit `BROKER_PUBLIC_HOST` en mémoire flash sans jamais la
redemander : les fusionner en une seule variable fonctionnerait parfaitement en
développement et imposerait ensuite un déplacement sur site pour chaque appareil
enrôlé.

Les seuils d'ingestion (`INGEST_MAX_FUTURE_SECONDS`, `INGEST_MAX_AGE_DAYS`,
`INGEST_MAX_BATCH`, …) valent par défaut ce qu'annonce le protocole gelé. En
modifier un change ce qui est promis aux connecteurs conformes : modifiez donc
le protocole d'abord.

## Mise en route

Le courtier n'est ni optionnel ni ordinaire : c'est Mosquitto avec le **plugin
dynamic-security**, car ce service crée et supprime des clients du courtier à
l'exécution. Utilisez la pile de développement, qui câble ensemble le courtier,
les deux bases de données et la passerelle :

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

L'API est alors sur <http://localhost:8008>, avec à ses côtés un conteneur
`live-data-worker` qui consomme le MQTT.

Un simulateur de connecteur permet de travailler sans matériel. Le port du
courtier n'est publié que sur la boucle locale de l'hôte, mais le plus simple
reste de lancer le simulateur dans la pile.

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev run --rm --no-deps \
  live-data python scripts/simulate_device.py --enroll-token ABCD-… --profile pv-day
```

Les profils couvrent une journée photovoltaïque, un arriéré de type
store-and-forward, un message par motif de rejet, une charge utile surdimensionnée
et une dérive d'horloge.

## Tests

```bash
pytest                    # nécessite un PostgreSQL Docker sur le port 5433
ruff check .
ruff format --check .
mypy .
```

`ruff check` et `ruff format --check` sont deux barrières distinctes : une ligne
de 110 caractères est une erreur de lint que le formateur considère déjà
formatée, et un guillemet mal choisi est l'inverse.

**Aucun test ne contacte un courtier, et c'est délibéré.** GitHub Actions crée
les conteneurs de service *avant* de récupérer le dépôt : un `mosquitto.conf`
versionné ne peut donc jamais leur servir de source de montage — et sans ce
fichier le courtier n'a aucune sécurité dynamique. L'adaptateur du courtier est
donc testé ici contre un transport factice, et la moitié qui exige un vrai
courtier vit dans le monorepo, dans `scripts/verify-live-ingest.sh` : elle
vérifie qu'un appareil révoqué est réellement déconnecté, qu'un statut retenu
est réellement effacé, et qu'un arriéré renvoyé écrase réellement.

## Schéma

`scripts/sql/schema.sql` est du DDL brut pour une base NEUVE ;
`scripts/sql/migrations/` contient les fichiers numérotés, unidirectionnels et
rejouables pour une base vivante. Il n'y a ni Alembic ni exécuteur — les
migrations s'appliquent à la main ou via `provision.sh`, dans l'ordre des noms.
Les mêmes instructions vivent aux deux endroits, et
`tests/test_schema_migration_parity.py` provisionne une base depuis chacun puis
compare des instantanés de catalogue normalisés, ce qui les maintient égaux.
`shared/models/local_models.py` reflète le schéma à la main : une
modification de l'un est une modification des deux. `measurement` est
partitionnée par mois, par intervalle, avec une partition DEFAULT ; une partition
par défaut non vide signifie que le travail de partitionnement s'est arrêté, et
`/health/readiness` le signale.

## Contribuer

Les contributions sont les bienvenues ! Merci de lire le
[guide de contribution](../CONTRIBUTING.md) et notre
[code de conduite](../CODE_OF_CONDUCT.md) (en anglais) avant d'ouvrir une issue
ou une pull request.

## Sécurité

Pour signaler une faille de sécurité, veuillez suivre la
[politique de sécurité](../SECURITY.md) — n'ouvrez pas d'issue publique.

## Licence

Ce projet est distribué sous la [licence Apache 2.0](../LICENSE).
