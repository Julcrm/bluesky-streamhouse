# Roadmap — Bluesky Streamhouse

> Fil rouge du projet, à relire en début de session et à mettre à jour en fin de session.
> Référence de structure et de propreté : [velib-lakehouse](https://github.com/Julcrm/velib-lakehouse).

**Phase en cours :** 1 — Ingestion : Jetstream → Redpanda
**Dernière mise à jour :** 2026-09-24

---

## Architecture cible

```
Bluesky Jetstream (WebSocket)
        │  Python producer
        ▼
Redpanda — topic unique `raw_events`
        │
        ├── (jours pairs / impairs) ───────────┐
        ▼                                      ▼
BRANCHE A — Enterprise Standard        BRANCHE B — Modern / Zero-JVM
PySpark Structured Streaming (5s)      Quix Streams
Apache Iceberg (manifests S3)          DuckLake (catalogue Postgres, inlining)
Garage S3 (Parquet)                    Garage S3 (Parquet)
dbt-spark                              dbt-duckdb
        │                                      │
        └──────────────────┬───────────────────┘
                           ▼
        Observabilité & Benchmark — Dagster · métriques RAM/CPU normalisées
        Dashboard (Streamlit ou Evidence) · API FastAPI pour le portfolio
```

**Question posée par le projet :** pour le même flux temps réel, que coûte (CPU, RAM, stockage, latence,
complexité opérationnelle) une stack JVM « enterprise » face à une stack zero-JVM moderne ?

---

## Structure cible du repo (miroir de velib-lakehouse)

```
src/
├── config.py                  # Constantes centralisées (URLs, topics, buckets, rétention)
├── ingestion/
│   └── producer.py            # Jetstream WebSocket → Redpanda `raw_events`
├── processing/
│   ├── spark/stream_job.py    # Branche A — Structured Streaming → Iceberg Bronze
│   └── quix/app.py            # Branche B — Quix Streams app → DuckLake Bronze
├── resources/                 # Ressources Dagster partagées
│   ├── s3.py                  # Garage (S3-compatible), decision D8
│   ├── redpanda.py
│   └── ducklake.py
├── dagster/
│   ├── assets.py              # Aucune logique métier — délègue aux modules
│   ├── definitions.py
│   └── sensors.py             # Alertes d'échec (Resend, comme velib)
├── maintenance/
│   ├── iceberg.py             # Compaction, expire snapshots, orphan files
│   └── ducklake.py            # Flush inlined data, merge files, expire snapshots
├── benchmark/
│   └── collector.py           # Collecte des métriques RAM/CPU/latence par branche
└── serving/
    └── api.py                 # FastAPI — métriques pour le portfolio

dbt/
├── spark/                     # Branche A (dbt-spark) — mêmes noms de modèles que duckdb/
└── duckdb/                    # Branche B (dbt-duckdb)

dashboard/                     # Streamlit ou Evidence
docker/
├── producer/Dockerfile
├── spark/Dockerfile
├── quix/Dockerfile
├── dagster/Dockerfile
└── api/Dockerfile
docker-compose.yaml            # Prod (réseau coolify) — services applicatifs seulement, l'infra est partagée (D9)
docker-compose.dev.yaml        # Local : Redpanda + Garage + Postgres
tests/                         # Un fichier de test par module
```

---

## Règles héritées de velib (et correctifs)

À garder :
- Assets Dagster sans logique métier, docstrings de module et de fonction partout (en anglais).
- En-tête `Model / Description / Source / Output` dans chaque modèle SQL, CTE nommées.
- Tests dbt génériques (`schema.yml`) et tests singuliers `assert_*.sql`.
- Dockerfiles commentés, uv, couche de dépendances mise en cache.
- CI : lint → format → tests → `uv lock --check` → deploy Coolify (seulement sur `main`).
- README avec les sections Architecture / Stack / Project structure / Tests / CI / Deployment.

À corriger par rapport à velib :
- [ ] Aucun bucket ni chemin en dur dans le SQL : `source()`, `var()` et `env_var()`.
- [ ] Aucun chemin absolu dans les assets : `Path(__file__)` ou config.
- [ ] Endpoints FastAPI qui appellent DuckDB en `def` (pas en `async def` bloquant).
- [ ] Horodatage en UTC et timezone-aware partout (`datetime.now(UTC)`).
- [ ] Pas de `defs` dupliqué, pas de code mort, entry points `[project.scripts]` valides.
- [ ] Stack locale reproductible (`docker-compose.dev.yaml`) et `.env.example` à jour.
- [ ] Logique métier en Gold uniquement, l'API ne fait que lire.
- [ ] Tests pour chaque module, dont `maintenance/`, et `dbt parse` en CI.
- [ ] Git : commits conventionnels, une branche par phase, PR vers `dev` puis `main`.

---

## Versions épinglées (vérifiées le 2026-09-24)

| Composant | Version | Contrainte |
|---|---|---|
| Python | **3.12** | pyspark, dbt, duckdb et dagster exigent ≥ 3.10 ; 3.12 est supporté partout |
| Java (image Spark) | 17 | Requis par Spark 4.1 |
| Spark / PySpark | **4.1.3** (Scala 2.13) | Pas 4.2 : Iceberg 1.11 ne fournit des runtimes que pour Spark 4.0 et 4.1 |
| Iceberg | `iceberg-spark-runtime-4.1_2.13:1.11.0` | |
| Iceberg AWS | `iceberg-aws-bundle:1.11.0` | |
| Kafka ↔ Spark | `spark-sql-kafka-0-10_2.13:4.1.3` | Même version que Spark |
| S3A (catalogue `hadoop`, checkpoints) | `hadoop-aws:3.4.2` + AWS SDK v2 `bundle:2.29.52` | Alignés sur la version de Hadoop embarquée par Spark 4.1.3 |
| dbt-core / dbt-spark[session] | 1.12.x / 1.11.x | `pyspark<5` accepté |
| dbt-duckdb / duckdb | 1.11.x / 1.5.x | Support DuckLake natif (`ducklake:` dans `attach`) |
| Quix Streams | 3.26.x | Python ≥ 3.9, dépend de confluent-kafka < 2.12 et rocksdict (wheels cp312 disponibles) |
| Dagster | 1.13.x | Python < 3.15 |

Les jars sont téléchargés au build de l'image Spark, jamais au runtime.
Montée vers Spark 4.2 : à réévaluer dès qu'Iceberg publie `iceberg-spark-runtime-4.2`.

---

## Décisions

| # | Décision | Options | Reco | Statut |
|---|---|---|---|---|
| D1 | Sens de l'alternance J / J-1 | (a) une branche en live par jour, en alternance · (b) A en live le jour J, B rejoue J-1 depuis Redpanda | (a) : A le jour 1, B le jour 2, comparaison normalisée par le volume | **tranché le 2026-09-24** |
| D2 | Collections Jetstream ingérées | posts seuls · posts + likes + reposts + follows | posts + likes + reposts + follows, volume mesuré en phase 1 | **tranché le 2026-09-24** |
| D3 | Versions Python / Spark | 3.12 + Spark 4.x · 3.11 + Spark 3.5 | Python 3.12 + Spark 4.1.3 (Spark 4.2 n'a pas encore de runtime Iceberg) | **tranché le 2026-09-24**, voir « Versions épinglées » |
| D4 | Collecte des métriques | Docker SDK (stats) · cAdvisor + Prometheus | Docker SDK : plus léger, suffisant sur un VPS | **tranché le 2026-09-24** |
| D5 | Dashboard | Portfolio React (via `serving/api.py`) · Streamlit · Evidence | Portfolio React existant, alimenté par l'API | reporté (phase 8) |
| D6 | dbt-spark : méthode de connexion | `session` · Thrift server | `session` : pas de service en plus | **tranché le 2026-09-24** |
| D7 | Moteur de la branche B | Bytewax · Quix Streams · Pathway · Arroyo | Quix Streams : Apache-2.0, releases actives, natif Kafka/Redpanda, 100 % Python sans JVM (Bytewax est maintenu par la communauté seulement depuis mai 2025, dernière release en nov. 2024) | **tranché le 2026-09-24 : Quix Streams** |
| D8 | Stockage S3 (MinIO archivé upstream) | Garder MinIO · Garage · SeaweedFS · RustFS | Garage : Rust, binaire unique, porté par une association. Région S3 obligatoire côté clients (`AWS_DEFAULT_REGION=us-east-1`) | **tranché et migré le 2026-09-24** : Garage en prod (ressource Coolify, compose hors de ce repo), MinIO supprimé le 2026-09-24 |
| D9 | Déploiement de Redpanda | Service Coolify séparé · intégré au compose du projet | Service séparé : infra partagée comme Garage/Postgres, pas coupé par les redéploiements du projet | **tranché le 2026-09-24**, ressource Coolify (compose hors de ce repo) |

Les décisions tranchées sont reportées dans le journal, avec leur justification.

---

## Phases

### Phase 0 — Cadrage & fondations
**Objectif :** un squelette de repo propre et une stack locale qui démarre.

- [x] Trancher D2 et D3
- [x] État de maintenance de Bytewax vérifié : maintenu par la communauté, pas de release depuis la 0.21.1 (nov. 2024), voir D7
- [x] Vérifier la matrice de compatibilité (voir « Versions épinglées »)
- [x] Créer l'arborescence cible (modules vides avec docstrings)
- [x] `pyproject.toml` avec des groupes de dépendances par service (`producer`, `spark`, `quix`, `dagster`, `api`) pour garder des images légères
- [x] `src/config.py` : topics, buckets, URLs Jetstream, rétention
- [x] `docker-compose.dev.yaml` : Redpanda + Redpanda Console (port 8088) + stockage S3 + Postgres, avec création du bucket et de la base `dagster`
- [x] Makefile : `up`, `down`, `reset`, `logs`, `ps` et `check_uv` en prérequis
- [x] `.env.example` complet, supprimer `src/main.py` et le test du template
- [x] Stack locale : Garage à la place de MinIO (bucket et clé créés automatiquement), `AWS_DEFAULT_REGION` dans `.env.example`, ports limités à `127.0.0.1`
- [x] CI verte sur GitHub (PR #1, mergée par erreur dans `main`, puis `dev` réaligné)

**Fini quand :** `make up` démarre la stack locale, et `make lint` et `make test` passent en CI.

### Phase 1 — Ingestion : Jetstream → Redpanda
**Objectif :** un flux continu, fiable et qui reprend là où il s'est arrêté.

- [x] `producer.py` : client WebSocket **Jetstream v2** (`/xrpc/network.bsky.jetstream.subscribeEvents`), filtres `collections` + `kinds=commit`
- [x] Reconnexion avec backoff exponentiel + jitter ; reprise au dernier `seq` lu **dans le topic** (pas d'état local), replay plafonné à 60 min
- [x] Coupure réseau en cours de session testée (règle pare-feu sur tcp/443 pendant 40 s) : détection en 15 s par keepalive (≤ 20 s), reprise au dernier `seq` acquitté, un seul doublon à la borne, 0 perte
- [x] Clé de message `did`, zstd, `acks=all`, idempotence, horodatage = heure de l'événement
- [x] Topic `raw_events` créé par le producer : 3 partitions, `retention.ms` = 24 h, `CreateTime`
- [x] Logs toutes les 30 s : msg/s, KiB/s, lag, dernier `seq` acquitté, erreurs de livraison
- [x] Tests unitaires : URL, parsing, choix du curseur (dont plafond de rejeu), backoff
- [x] `docker/producer/Dockerfile` (Python 3.12.14-slim, uv 0.12.18, groupe `producer`, non-root, arrêt propre sur SIGTERM) + `make produce`
- [x] Volume mesuré (voir journal)
- [ ] Déployer le producer sur le VPS (service Coolify, `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092`) pour le run de 24 h

**Fini quand :** 24 h d'ingestion sans trou, et un redémarrage du producer ne perd pas d'événements.
**Piège :** au premier démarrage, le rejeu Jetstream peut inonder Redpanda ; limiter la fenêtre du curseur.

### Phase 2 — Branche B : Quix Streams → DuckLake (Bronze)
**Objectif :** écriture streaming vers DuckLake.

- [ ] `resources/ducklake.py` : `ATTACH 'ducklake:postgres:…'` avec `DATA_PATH` sur Garage
- [ ] Application Quix Streams : topic `raw_events` → parsing (StreamingDataFrame) → `BatchingSink` custom vers DuckLake (taille et délai de batch)
- [ ] Configurer le data inlining (petits commits dans Postgres) et mesurer son effet
- [ ] Idempotence : commit des offsets après écriture du batch (at-least-once) et dédoublonnage sur la clé naturelle `(did, collection, rkey, time_us)`
- [ ] `docker/quix/Dockerfile`
- [ ] Tests unitaires des étapes de transformation (sans Kafka)

**Fini quand :** la table Bronze se remplit en continu et un redémarrage ne crée pas de trou.

### Phase 3 — Branche B : dbt-duckdb (Silver / Gold) + Dagster
**Objectif :** medallion complet sur la branche B, orchestré.

- [ ] Silver : typage par collection (`posts`, `likes`, `reposts`, `follows`), dédoublonnage, gestion des `delete`
- [ ] Gold : posts/minute, langues (`langs`), hashtags (facets), utilisateurs actifs, engagement
- [ ] Tests dbt : `schema.yml` et `assert_*.sql`
- [ ] Assets Dagster : `quix_bronze` (observable), `duckdb_silver`, `duckdb_gold`
- [ ] `maintenance/ducklake.py` : flush inlined, merge adjacent files, expire snapshots, cleanup old files
- [ ] Sensor d'échec avec alerte mail

**Fini quand :** Dagster matérialise Silver/Gold sur un schedule et les tests dbt passent.

### Phase 4 — Branche A : PySpark → Iceberg (Bronze)
**Objectif :** le même Bronze, en version JVM.

- [ ] Session Spark 4.1.3 : catalogue Iceberg `hadoop` sur `s3a://`, jars épinglés (voir « Versions épinglées »)
- [ ] Structured Streaming : source Kafka, `trigger(processingTime="5 seconds")`, checkpoints sur Garage
- [ ] Même schéma Bronze que la branche B
- [ ] `docker/spark/Dockerfile` (JVM, jars pré-téléchargés dans l'image, pas au runtime)
- [ ] Tests des transformations avec une SparkSession locale

**Fini quand :** la table Iceberg Bronze se remplit et reprend proprement depuis le checkpoint.
**Pièges :** un trigger de 5 s donne environ 17 000 commits/jour, donc explosion des petits fichiers et des métadonnées (la maintenance est obligatoire). Le catalogue `hadoop` supporte un seul writer.

### Phase 5 — Branche A : dbt-spark (Silver / Gold) + Dagster
**Objectif :** parité fonctionnelle avec la branche B.

- [ ] Modèles dbt-spark avec **les mêmes noms et colonnes** que dbt-duckdb
- [ ] Mêmes tests dbt
- [ ] Assets Dagster : `spark_bronze`, `spark_silver`, `spark_gold`
- [ ] `maintenance/iceberg.py` : `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`

**Fini quand :** les deux branches exposent les mêmes tables Gold (mêmes noms, mêmes colonnes) et passent les mêmes tests dbt.

### Phase 6 — Alternance jour 1 / jour 2
**Objectif :** faire tourner une seule branche par jour, à tour de rôle (D1).

- [ ] Schedule Dagster de bascule quotidienne à 00:00 UTC : arrêt de la branche sortante, démarrage de la branche entrante
- [ ] Chaque branche démarre à l'offset de 00:00 (`offsets_for_times`) sur son propre consumer group : aucun trou ni doublon à la bascule
- [ ] Table `branch_calendar` (date, branche active, offsets de début et de fin) comme référence du benchmark
- [ ] Contrôle de complétude : nombre d'événements dans Bronze égal au nombre d'offsets consommés dans Redpanda pour la journée (asset check Dagster)
- [ ] Sensors d'échec sur tous les jobs

**Fini quand :** 14 jours d'alternance automatique sans intervention. Avec un cycle de 2 jours sur une semaine de 7, chaque branche passe par les 7 jours de la semaine en 14 jours.
**Piège :** la journée de bascule mélange deux branches si l'arrêt n'est pas propre. Il faut attendre le dernier commit de la branche sortante avant de figer l'offset de fin.

### Phase 7 — Observabilité & benchmark
**Objectif :** des chiffres comparables et défendables.

**Règle d'or :** on ne compare jamais de totaux bruts, seulement des ratios d'efficience par message,
calculés sur la même fenêtre glissante de 5 minutes pour les deux branches.

| On évite (brut, biaisé par le trafic) | On retient (normalisé) | Calcul sur la fenêtre de 5 min |
|---|---|---|
| RAM totale du conteneur | **Mo·s de RAM par tranche de 10 000 messages**, et RAM à débit équivalent | ∫ RSS dt ÷ messages × 10 000 |
| Charge CPU globale (%) | **ms de CPU par message** | Δ temps CPU cgroup ÷ messages |
| Volume disque total écrit | **Ko par message stocké (Parquet)**, par collection | Δ octets Bronze ÷ messages, après compaction |
| Durée totale du batch | **Débit max soutenu (msg/s/vCPU)** et latence p50/p95 | Test de rattrapage (voir ci-dessous) |

- [ ] `benchmark/collector.py` : échantillonne toutes les 10 s CPU (temps cgroup cumulé) et RSS du conteneur actif, et agrège en fenêtres de 5 min
- [ ] **Source unique pour le nombre de messages**, identique pour les deux branches : lignes écrites dans Bronze par fenêtre (colonne `processed_at`), recoupées avec les offsets Redpanda. Spark ne commit pas ses offsets dans un consumer group (ils sont dans son checkpoint), donc les offsets Redpanda seuls ne suffisent pas.
- [ ] Table `benchmark_windows` (branche, début de fenêtre, messages, cpu_ms, ram_mb_s, bytes_written, lag, latence p50/p95, mix de collections)
- [ ] **Buckets de débit** : ranger chaque fenêtre par niveau de trafic (ex. 0–200, 200–500, 500+ msg/s) et comparer A et B bucket par bucket. C'est ce qui rend « à volume équivalent » rigoureux.
- [ ] **Coût fixe à part** : RAM et CPU du conteneur au repos (JVM, runtime Quix) mesurés séparément et affichés comme une ligne de base
- [ ] **Test de rattrapage** hebdomadaire : consumer en pause 30 min, puis vitesse de résorption du lag. C'est la seule mesure honnête du débit max ; en temps normal, les deux branches suivent simplement le rythme du flux.
- [ ] Limites CPU/RAM Docker identiques pour les deux branches
- [ ] Comparer aussi à jour de semaine équivalent (lundi A contre lundi B)
- [ ] Optionnel : les deux branches rejouent le même échantillon d'1 h, pour valider la normalisation et vérifier la parité des tables Gold

**Fini quand :** 14 jours de fenêtres (7 par branche) stockés, avec des résultats stables d'une semaine à l'autre.
**Piège :** la RAM est un niveau, pas un flux. « Mo par 10 000 messages » calculé naïvement (RAM moyenne ÷ messages) explose la nuit quand le trafic chute. D'où le Mo·s (intégrale dans le temps, comme la facturation cloud) et la comparaison par bucket de débit.

### Phase 8 — Dashboard & serving
**Objectif :** rendre le benchmark visible, en direct.

- [ ] En haut : stats live de la branche active (débit, ms CPU/msg, Mo·s/10k msg, lag, latence), rafraîchies toutes les 5 min
- [ ] En bas : courbes superposées des ratios d'efficience, par exemple « Efficience RAM : Quix Streams (aujourd'hui) vs PySpark (hier) à volume équivalent », avec un axe par bucket de débit
- [ ] Encart coût fixe (conteneur au repos) et encart débit max (test de rattrapage)
- [ ] Tendances Gold Bluesky (langues, hashtags, activité)
- [ ] `serving/api.py` : endpoints benchmark et insights (clé API, `/health`, comme velib)
- [ ] Données prêtes pour une page projet du portfolio (PipelineViz)

### Phase 9 — Déploiement VPS (Coolify)
- [x] Déployer Redpanda comme service Coolify séparé (D9) (vérifié en prod le 2026-09-24)
- [x] Migration MinIO → Garage (D8) en prod : POC, `rclone sync` + `check` (0 différence), bascule de velib et Dagster
- [x] MinIO supprimé et 9000/9001 retirés du pare-feu (2026-09-24)
- [ ] Job `deploy` en CI via Tailscale (OIDC, `tag:ci`), sur le modèle de velib : voir la note Obsidian « VPS - Déploiement CI via Tailscale »
- [ ] `docker-compose.yaml` prod sur le réseau `coolify`, avec `mem_limit` et `cpus` : producer, spark, quix, dagster, api (Garage, Postgres et Redpanda sont des services Coolify partagés)
- [ ] Secrets dans Coolify, aucun en clair
- [ ] Rétention Garage et Redpanda dimensionnées pour le disque du VPS

**Fini quand :** le pipeline tourne en prod et un push sur `main` redéploie automatiquement.

### Phase 10 — Documentation & valorisation
- [ ] README : Architecture, Stack, Project structure, **Benchmark results**, Tests, CI, Deployment
- [ ] `docs/adr/` : une ADR par décision D1 à D6
- [ ] Rédaction des conclusions du benchmark (article ou page portfolio)

---

## Journal de sessions

> Une entrée par session : ce qui a été fait, les décisions prises, la suite.

### 2026-09-24 — Session 1
- Analyse de velib-lakehouse (conventions à garder, 15 points à corriger)
- Définition de l'architecture et rédaction de cette roadmap
- **D1 tranché :** alternance, branche A le jour 1 et branche B le jour 2, performances comparées par rapport au volume de données. Pas de rejeu, donc une rétention Redpanda de 24 h suffit.
- **Méthodo benchmark actée :** uniquement des ratios par message sur des fenêtres de 5 min (Mo·s RAM/10k msg, ms CPU/msg, Ko/msg Parquet, msg/s/vCPU), comparés par bucket de débit, avec le coût fixe mesuré à part et un test de rattrapage pour le débit max.
- **Bytewax vérifié :** maintenu par la communauté depuis mai 2025, aucune release PyPI depuis la 0.21.1 (nov. 2024). Le support de Python 3.13/3.14 a été mergé en juin 2026 mais n'est pas publié. **D7 tranché : Quix Streams.**
- **D2, D4 et D6 tranchés :** posts + likes + reposts + follows ; Docker SDK ; dbt-spark en méthode `session`. D5 reporté : le dashboard ira dans le portfolio React existant, via l'API.
- **D3 tranché après vérification sur PyPI et Maven Central :** Python 3.12 + Spark 4.1.3. Spark 4.2.0 (juillet 2026) est écarté faute de runtime Iceberg. Python 3.9/3.10 inutile : 3.12 est supporté par toute la stack, et 3.9 est même exclu par pyspark, dbt, duckdb et dagster.
- **Squelette de la phase 0 posé** sur `feat/phase-0-skeleton` : arborescence, config, groupes uv, stack locale validée (`make up`, bucket et bases créés), lint, format et tests OK.
- **MinIO archivé upstream** (repo GitHub archivé, image retirée de Docker Hub) : dernière image quay.io utilisée en local, voir D8.
- **D9 tranché :** Redpanda en service Coolify séparé. Compose prêt dans `infra/redpanda/` et testé en local : santé du cluster OK, auto-création des topics désactivée, Kafka et Console exposés uniquement sur 127.0.0.1 (accès par tunnel SSH), environ 190 Mo de RAM au repos.
- **D8 tranché :** Garage remplacera MinIO, migration plus tard.
- **Redpanda déployé sur le VPS et vérifié :** cluster sain, `redpanda:9092` joignable depuis le réseau `coolify`, ports 19092 et 8088 limités à 127.0.0.1, Console connectée, environ 240 Mo de RAM. VPS : 4 vCPU, 7,8 Go de RAM (environ 4,8 Go disponibles), 215 Go de disque libre, à garder en tête pour Spark.
- **Redpanda Console via Tailscale :** http://100.125.33.49:8088, avec `CONSOLE_BIND_IP` défini dans Coolify. Injoignable depuis l'IP publique. `net.ipv4.ip_nonlocal_bind=1` persisté sur le VPS (`/etc/sysctl.d/99-nonlocal-bind.conf`) pour que la console démarre même si Tailscale n'est pas encore prêt au boot.
- **Migration MinIO → Garage en prod (D8)** : compose dans `infra/garage/`. POC depuis `velib-api`, copie rclone (795 objets, 0 différence), bascule de velib et Dagster. Piège : Garage exige la région S3, d'où `AWS_DEFAULT_REGION=us-east-1` dans tous les clients (lue par s3fs et DuckDB, aucun code modifié).
- **Incident hors migration** : `opendata.paris.fr` en NXDOMAIN depuis le 2026-09-23, donc velib en échec depuis le 23/09 à 11:20. Correctif `baa9aa6` dans velib (hôte `parisdata.opendatasoft.com`).
- **CI → Coolify via Tailscale** : les routes publiques de Coolify ont été retirées. Les runners GitHub rejoignent le tailnet en `tag:ci` (OIDC, sans secret longue durée), avec accès limité à `100.125.33.49:8000`. Validé sur velib (`23d04d3`). Schéma à réutiliser en phase 9.
- **Suite :** commit de la phase 0 (+ Garage dans la stack locale), PR vers `dev`, puis phase 1
- **Phase 0 close** : CI verte, `dev` réaligné sur `main`. Template cookiecutter corrigé (lint de `main.py`).

### 2026-09-24 — Phase 1 (ingestion)
- **Jetstream réécrit (v2)** : nouveau endpoint `/xrpc/network.bsky.jetstream.subscribeEvents`, paramètres `collections` et `kinds`, enveloppe `{"$type": "message", "payload": {...}}`, curseur = `seq` **inclusif**, livraison at-least-once. Les événements `identity`/`account` ignorent le filtre de collections, d'où `kinds=commit`.
- **Volume mesuré** (4 collections) : ~410-460 msg/s, ~600 B/msg JSON, soit ~37 M msg/jour et ~23 Go/jour bruts. Mix : 60 % likes, 11 % posts, 10 % reposts, 8 % follows, 9 % suppressions de posts. En Redpanda (zstd) : **184 B/msg (×3,3)**, soit **~6,8 Go pour 24 h** de rétention.
- **Reprise prouvée** sur 225 969 messages : un seul `seq` dupliqué, exactement la borne de reprise, donc aucun trou. Dédoublonnage aval sur `seq`.
- **Partitions déséquilibrées** (×2,5 sur une partition) : quelques DID très actifs (probablement des bots). À surveiller pour Spark et Quix.
- **Piège** : `localhost` → librdkafka tente IPv6 (`::1`) en premier. En local, Redpanda annonce `127.0.0.1:19092`.
- **Test de coupure** : 1er essai, détection en 35 s (ping par défaut 20+20 s) et 3 doublons (accusés de réception non traités). Corrigé avec ping 10+10 s, `flush()` avant de choisir le curseur, et backoff remis à zéro après une session saine. 2e essai : détection en 15 s, 1 seul doublon, 0 perte.
- **Prod prête** : `docker-compose.yaml` (producer seul, réseau `coolify`, limite 256 Mo), job `deploy` via Tailscale/OIDC, ignoré tant que les secrets ne sont pas configurés. Mesure : ~37 Mio de RAM, ~11 % d'un CPU en régime normal, ~50 % en rattrapage (~4 900 msg/s). File librdkafka plafonnée à 64 Mo.
- **Suite** : merge `dev` → `main`, création de la ressource Coolify, secrets CI, puis run de 24 h
- **`infra/` retiré du repo** (choix de Julien) : les composes Redpanda et Garage partagés vivent dans Coolify. Dernière version versionnée : commit `e4964a6`.
