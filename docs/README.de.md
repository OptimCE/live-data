<p align="center">
  <img src="logo.svg" alt="OptimCE Logo" width="160">
</p>

# OptimCE — Live-Daten

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Lizenz](https://img.shields.io/badge/Lizenz-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-43a047.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](README.nl.md)

Viertelstundenwerte intelligenter Zähler, über MQTT.

Heute erreichen die Zählerdaten eines Mitglieds OptimCE über den
Verteilernetzbetreiber: Monate später und als Monatsdatei. Das genügt zum
Abrechnen und taugt für nichts anderes — eine Gemeinschaft kann nicht sehen, ob
ihre Solarproduktion *gerade jetzt* lokal verbraucht wird, und einem Mitglied
lässt sich nicht sagen, dass die Spülmaschine in der nächsten Stunde
kostenlos läuft. Dieser Dienst ist der andere Weg: ein Konnektor im
Zählerschrank, der Messwerte veröffentlicht, sobald sie anfallen.

Er ist ein **Annex-Dienst**: Er besitzt seine eigene Datenbank, liest das
zentrale CRM nur lesend und wird mit dem Rest der Plattform im
[Monorepo](https://github.com/OptimCE/monorepo) zusammengesetzt.

> **Umfang.** Phase 1 ist abgeschlossen — alle elf Bauschritte: Erfassung,
> Registrierung, Geräteverwaltung, Eigentumsprojektion, Aggregationen und
> ihr Scheduler, Lese-API, Betriebsoberfläche und die Prognose-*Schnittstelle*.
> **Es wird keine Prognosemethode ausgeliefert**, bewusst: `forecasting/` ist
> der Erweiterungspunkt und `forecasting/methods_implemented/` ist leer.
> Siehe [CHANGELOG.md](../CHANGELOG.md).

## Das Protokoll ist ein veröffentlichter, eingefrorener Vertrag

Das meiste in diesem Repository darf sich frei ändern. Das Format auf der
Leitung nicht.

`docs/live-data-protocol.md` im Monorepo ist **auf `v1` eingefroren**. Drei
unabhängige Konnektoren implementieren es von außerhalb dieses Repositorys, und
eine Firmware legt ihre Registrierungsantwort im Flash ab und fragt nie wieder
nach — für einen Kasten in einem Keller gibt es kein Fernupdate. §7 jenes
Dokuments regelt, was sich ändern darf: ein neues *optionales* Feld ist
innerhalb von `v1` erlaubt; ein neues Pflichtfeld oder eine geänderte Bedeutung
eines bestehenden Feldes ist `v: 2`.

Die Ablehnungssystematik hier und §4.2 dort müssen in beide Richtungen
übereinstimmen, in Namen **und** Geltungsbereich. Ändern Sie das eine, ändern
Sie das andere.

## Wie ein Messwert ankommt

Ein Konnektor veröffentlicht auf zwei Topics und abonniert nichts:

```
ce/{community_id}/{device_id}/telemetry    QoS 1
ce/{community_id}/{device_id}/status       QoS 0, retained, zugleich das Last Will
```

Der Fluss ist bewusst einseitig. Es gibt kein Kommando-Topic und keine
Bestätigung, die ein Konnektor lesen könnte — genau das erlaubt ihm den Betrieb
auf Hardware ohne Rückkanal, und genau deshalb gibt es die unten beschriebenen
Ablehnungszähler.

Eine Telemetrienachricht trägt bis zu 200 Messwerte in einem einzigen Array; ein
Gerät, das offline war, sendet seinen Rückstand also in einer einzigen
Veröffentlichung. Die Speicherung ist idempotent über `(Gerät, ts)`: **ein
erneut gesendeter Messwert überschreibt den vorhandenen**. Dieses Versprechen
macht QoS 1 ausreichend und macht „ich bin nicht sicher, ob das angekommen ist,
ich sende noch einmal" zum richtigen Verhalten eines Konnektors statt zu einem
Risiko.

## Ablehnungen haben einen Geltungsbereich

Eine fehlerhafte Nachricht wird still abgelehnt, unter einem Grund gezählt und
protokolliert. Jeder Grund ist danach eingeordnet, was er verwirft:

| Bereich | Verwirft | Gründe |
|---|---|---|
| Nachricht | alles in der Veröffentlichung | `schema_invalid`, `unknown_field`, `batch_too_large`, `duplicate_ts_in_batch`, `device_unknown`, `device_revoked`, `community_mismatch` |
| Messwert | einen Messwert; der Rest des Stapels wird gespeichert | `ts_in_future`, `ts_too_old`, `ts_not_aligned`, `negative_energy`, `over_device_ceiling`, `implausible_production` |

Diese Unterscheidung ist der Kern. Zwei Wochen Rückstand mit einem einzigen
Messwert aus einer abgedrifteten Uhr müssen den Messwert verlieren, nicht die
zwei Wochen. Ablehnungen auf Nachrichtenebene werden nach `ingest_dead_letter`
geschrieben; solche auf Messwertebene landen in
`device_last.last_reject_reason`.

Zwei Umstände werden gezählt, aber nie abgelehnt: ein Intervall, das zugleich
Bezug und Einspeisung trägt (an einem bewölkten Nachmittag völlig normal), und
ein unbekannter Schlüssel *innerhalb* eines Messwerts — §7 verbietet, diese
abzulehnen, und genau das macht das Hinzufügen eines optionalen Feldes sicher.

## Registrierung

Ein Gerät wird von einer Gemeinschaftsverwalterin oder einem
Gemeinschaftsverwalter angelegt, die oder der einen kurzen Code erhält. Jemand
am Zähler tippt diesen Code einmal in den Konnektor ein:

```
POST /live-public/enroll     { "token": "…", "connector": { … } }
-> { "broker": {…}, "credentials": {…}, "topics": {…} }
```

Dies ist der **einzige nicht authentifizierte Endpunkt** der Plattform. Das
Token besteht aus 128 Bit CSPRNG-Ausgabe in Crockford-Base32, wird als
SHA-256-Hash gespeichert und ist einmal verwendbar. Das Alphabet bildet die beim
Vorlesen typischen Verwechslungen ab — `I` und `L` auf `1`, `O` auf `0` —, denn
der realistische Fall ist ein am Telefon diktierter Code, der an einem kalten
Abend in ein Captive Portal eingegeben wird.

**Das Passwort wird einmal angezeigt und ist nie wiederherstellbar.** OptimCE
speichert es nicht; der Broker hält nur einen Hash. Ein verlorenes Geheimnis
bedeutet eine erneute Registrierung — deshalb ist der eindeutige Index über die
Zähler einer Gemeinschaft partiell: ein widerrufenes Gerät gibt seine EAN wieder
frei.

Ein abgelaufenes Token, ein bereits verbrauchtes Token und ein Token, das es nie
gab, erzeugen die *identische* Antwort. Eine unterscheidbare Antwort würde einem
Angreifer verraten, dass eine Vermutung richtig war.

## Widerruf

Sofort und serverseitig: Der Broker-Client wird deaktiviert, die retained
`status`-Nachricht gelöscht und der Client entfernt. Ein widerrufenes Gerät wird
binnen Millisekunden getrennt, und seine Wiederverbindung wird abgelehnt. Es
gibt keine Aktion auf Geräteseite und keine Benachrichtigung — aus Sicht des
Konnektors hören seine Zugangsdaten schlicht auf zu funktionieren.

Das Löschen des retained Status ist der Schritt, den man leicht vergisst und
teuer vergisst: Ein `status`, der sein Gerät überlebt, wird bei jeder
Wiederverbindung erneut an den Erfassungs-Worker ausgeliefert, sodass der
Offline-Alarm bei jedem Deployment auslöst, bis das Team ihn nicht mehr liest.

## Endpunkte

| Methode | Pfad | Zweck |
|---|---|---|
| `GET` | `/live/version` | Build- und Schemaversion |
| `GET` | `/live/devices` | die Geräte der Gemeinschaft |
| `POST` | `/live/devices` | eines anlegen — prüft die EAN gegen das CRM |
| `POST` | `/live/devices/{id}/token` | einen Registrierungscode ausstellen (entwertet einen ungenutzten) |
| `POST` | `/live/devices/{id}/revoke` | widerrufen |
| `GET` | `/live/devices/{id}/diagnostics` | warum ein Gerät schweigt — Zustand, `diag`, Konnektor |
| `GET` | `/live/summary` | das aktuelle Signal der Gemeinschaft. **MITGLIED-Untergrenze** |
| `GET` | `/live/series` | eine aggregierte Reihe. **MITGLIED-Untergrenze** |
| `GET` | `/live/settings` | Sichtbarkeit und k. Liefert die Vorgaben, ohne zu schreiben |
| `PUT` | `/live/settings` | sie ersetzen |
| `GET` | `/live/forecast` | leer **mit benanntem Grund**, solange keine Methode existiert |
| `GET` | `/live/forecast/methods` | die Registry, derzeit leer |
| `GET` | `/live/ops/health` | die Flotte und das Alter der Aggregationen |
| `POST` | `/live-public/enroll` | **öffentlich.** Code gegen Zugangsdaten tauschen |

### Authentifizierung

Alles unter `/live` wird am Gateway authentifiziert, das den JWT prüft und
`x-user-id`, `x-community-id` sowie `x-user-orgs` weiterreicht. Der Dienst
vertraut diesen Headern und begrenzt jede Abfrage auf die Gemeinschaft der
aufrufenden Person; zusätzlich ist der Zugang an ein aktives Abonnement der
Gemeinschaft gebunden. Solange dieses Abonnement inaktiv ist, verwirft der Worker
auch die Telemetrie der Gemeinschaft (Statusmeldungen werden weiter verarbeitet)
und der Scheduler überspringt sie, sobald ihre ausstehenden Aggregationen erledigt
sind; Geräte und Verlauf bleiben erhalten, sodass eine Reaktivierung keine neue
Registrierung erfordert.

`/live-public/enroll` ist die Ausnahme und wird über einen eigenen
Gateway-Eintrag ohne Validator erreicht. Das Gateway kann die Vertrauensheader
nicht für diese eine Route entfernen, daher leert der Reverse Proxy sie, bevor
die Anfrage ankommt — ein Gerät hält ein Registrierungstoken, niemals eine
Sitzung.

## Konfiguration

| | |
|---|---|
| `CRM_DATABASE_URL` | das zentrale CRM, nur lesend |
| `LOCAL_DATABASE_URL` | die eigene Datenbank dieses Dienstes |
| `SUBSCRIPTION_CACHE_TTL_SECONDS` | wie lange Worker und Scheduler ihrer Kopie der abonnierten Gemeinschaften vertrauen (Standard 60, 1–3600) |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_TLS` | der Broker, den **dieser Dienst anwählt** |
| `MQTT_ADMIN_USERNAME` / `_PASSWORD` | die dynamic-security-Steuerverbindung |
| `MQTT_INGEST_USERNAME` / `_PASSWORD` | die Abonnentenidentität des Workers |
| `BROKER_PUBLIC_HOST`, `_PORT`, `_TLS` | die Adresse, die **einem Gerät genannt wird** |

Die letzten beiden Zeilen bezeichnen zwei verschiedene Dinge und müssen das
bleiben. Ein Gerät schreibt `BROKER_PUBLIC_HOST` ins Flash und fragt nie wieder
nach; sie zu einer Variablen zusammenzufassen, funktionierte in der Entwicklung
tadellos und erforderte danach einen Vor-Ort-Besuch bei jedem registrierten
Gerät.

Die Erfassungsschwellen (`INGEST_MAX_FUTURE_SECONDS`, `INGEST_MAX_AGE_DAYS`,
`INGEST_MAX_BATCH`, …) entsprechen standardmäßig den Werten des eingefrorenen
Protokolls. Einen davon zu ändern, ändert, was konformen Konnektoren zugesagt
wurde — ändern Sie also zuerst das Protokoll.

## Ausführen

Der Broker ist weder optional noch gewöhnlich: Mosquitto mit dem
**dynamic-security-Plugin**, denn dieser Dienst legt Broker-Clients zur Laufzeit
an und löscht sie. Nutzen Sie den Entwicklungs-Stack, der Broker, beide
Datenbanken und das Gateway zusammenschaltet:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

Die API liegt dann unter <http://localhost:8008>, daneben ein Container
`live-data-worker`, der MQTT konsumiert.

Für die Arbeit ohne Hardware gibt es einen Konnektor-Simulator. Der Port des
Brokers ist nur über das Loopback des Hosts veröffentlicht, am einfachsten läuft
der Simulator aber innerhalb des Stacks:

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev run --rm --no-deps \
  live-data python scripts/simulate_device.py --enroll-token ABCD-… --profile pv-day
```

Die Profile decken einen PV-Tag ab, einen Store-and-Forward-Rückstand, je eine
Nachricht pro Ablehnungsgrund, eine übergroße Nutzlast und Uhrendrift.

## Tests

```bash
pytest                    # benötigt ein Docker-PostgreSQL auf Port 5433
ruff check .
ruff format --check .
mypy .
```

`ruff check` und `ruff format --check` sind getrennte Prüfungen: Eine Zeile mit
110 Zeichen ist ein Lint-Fehler, den der Formatierer als bereits formatiert
ansieht — und falsch gewählte Anführungszeichen sind der umgekehrte Fall.

**Kein Test kontaktiert einen Broker, und das ist Absicht.** GitHub Actions legt
Service-Container an, *bevor* das Repository ausgecheckt wird; eine im
Repository versionierte `mosquitto.conf` kann deren Bind-Mount-Quelle daher nie
sein — und ohne diese Konfiguration hat der Broker überhaupt keine dynamische
Sicherheit. Der Broker-Adapter wird hier deshalb gegen einen Fake-Transport
getestet, und die Hälfte, die einen echten Broker braucht, liegt im Monorepo als
`scripts/verify-live-ingest.sh`: Sie belegt, dass ein widerrufenes Gerät
tatsächlich getrennt wird, dass ein retained Status tatsächlich gelöscht wird
und dass ein erneut gesendeter Rückstand tatsächlich überschreibt.

## Schema

`scripts/sql/schema.sql` ist rohes DDL für eine FRISCHE Datenbank;
`scripts/sql/migrations/` enthält die nummerierten, ausschließlich
vorwärtsgerichteten und wiederholbaren Dateien für eine laufende. Es gibt
weder Alembic noch einen Runner — Migrationen werden von Hand oder durch
`provision.sh` in Namensreihenfolge angewendet. Dieselben Anweisungen stehen an
beiden Orten, und `tests/test_schema_migration_parity.py` provisioniert je eine
Datenbank daraus und vergleicht normalisierte Katalog-Snapshots, was sie gleich
hält. `shared/models/local_models.py` bildet das Schema von Hand nach: eine
Änderung am einen ist eine Änderung an beidem. `measurement` ist nach Monat
bereichspartitioniert, mit einer DEFAULT-Partition; eine nicht leere
Standardpartition bedeutet, dass die Partitionierung stehen geblieben ist, und
`/health/readiness` meldet das.

## Mitwirken

Beiträge sind willkommen! Bitte lesen Sie die
[Beitragsrichtlinien](../CONTRIBUTING.md) und unseren
[Verhaltenskodex](../CODE_OF_CONDUCT.md) (auf Englisch), bevor Sie ein Issue
oder einen Pull Request eröffnen.

## Sicherheit

Um eine Sicherheitslücke zu melden, folgen Sie bitte der
[Sicherheitsrichtlinie](../SECURITY.md) — bitte eröffnen Sie kein öffentliches
Issue.

## Lizenz

Dieses Projekt steht unter der [Apache-Lizenz 2.0](../LICENSE).
