<p align="center">
  <img src="logo.svg" alt="OptimCE-logo" width="160">
</p>

# OptimCE — Live-gegevens

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be)
[![Licentie](https://img.shields.io/badge/Licentie-Apache%202.0-blue.svg)](../LICENSE)
[![en](https://img.shields.io/badge/lang-en-lightgrey.svg)](../README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-43a047.svg)](README.nl.md)

Kwartierwaarden van slimme meters, via MQTT.

Vandaag bereiken de meetgegevens van een lid OptimCE via de
distributienetbeheerder: maanden later, in een maandbestand. Dat volstaat om te
factureren en is verder nutteloos — een gemeenschap kan niet zien of haar
zonneproductie *nu* lokaal wordt verbruikt, en je kunt een lid niet vertellen
dat de vaatwasser het komende uur gratis draait. Deze dienst is de andere weg:
een connector in de meterkast die metingen publiceert zodra ze er zijn.

Het is een **annexdienst**: hij heeft zijn eigen databank, leest de centrale
CRM-databank alleen-lezen, en wordt met de rest van het platform samengebracht
in de [monorepo](https://github.com/OptimCE/monorepo).

> **Reikwijdte.** Fase 1 is voltooid — alle elf bouwstappen: inname,
> inschrijving, toestelbeheer, eigendomsprojectie, aggregaties en hun
> scheduler, lees-API, beheersoverzicht en de voorspellings-*aanhechting*.
> **Er wordt geen voorspellingsmethode geleverd**, bewust: `forecasting/` is
> het uitbreidingspunt en `forecasting/methods_implemented/` is leeg. Zie
> [CHANGELOG.md](../CHANGELOG.md).

## Het protocol is een gepubliceerd, bevroren contract

Het meeste in deze repository mag vrij veranderen. Het formaat op de lijn niet.

`docs/live-data-protocol.md` in de monorepo is **bevroren op `v1`**. Drie
onafhankelijke connectoren implementeren het van buiten deze repository, en een
firmware bewaart haar inschrijvingsantwoord in flash en vraagt er nooit meer
naar — voor een kastje in een kelder bestaat geen update op afstand. §7 van dat
document bepaalt wat mag wijzigen: een nieuw *optioneel* veld mag binnen `v1`;
een nieuw verplicht veld, of een gewijzigde betekenis van een bestaand veld, is
`v: 2`.

De afwijzingstaxonomie hier en §4.2 daar moeten in beide richtingen
overeenkomen, in namen **en** reikwijdte. Wijzigt u het ene, wijzig dan ook het
andere.

## Hoe een meting binnenkomt

Een connector publiceert op twee topics en abonneert zich op niets:

```
ce/{community_id}/{device_id}/telemetry    QoS 1
ce/{community_id}/{device_id}/status       QoS 0, retained, tevens de Last Will
```

De stroom is bewust eenrichtingsverkeer. Er is geen commandotopic en geen
bevestiging die een connector kan lezen, en juist dat laat hem draaien op
hardware zonder terugkanaal — en het is de reden dat de afwijzingstellers
hieronder bestaan.

Eén telemetriebericht draagt tot 200 metingen in één array, zodat een toestel
dat offline is geweest zijn achterstand in één publicatie verstuurt. De opslag
is idempotent op `(toestel, ts)`: **een opnieuw verzonden meting overschrijft de
bestaande**. Die belofte maakt QoS 1 toereikend en maakt van "ik weet niet zeker
of dat is aangekomen, ik stuur het opnieuw" het juiste gedrag van een connector
in plaats van een risico.

## Afwijzingen hebben een reikwijdte

Een misvormd bericht wordt stil afgewezen, geteld onder een reden en
gelogd. Elke reden is ingedeeld naar wat hij weggooit:

| Reikwijdte | Gooit weg | Redenen |
|---|---|---|
| bericht | alles in de publicatie | `schema_invalid`, `unknown_field`, `batch_too_large`, `duplicate_ts_in_batch`, `device_unknown`, `device_revoked`, `community_mismatch` |
| meting | één meting; de rest van de reeks wordt bewaard | `ts_in_future`, `ts_too_old`, `ts_not_aligned`, `negative_energy`, `over_device_ceiling`, `implausible_production` |

Dat onderscheid is de kern. Veertien dagen achterstand met één meting van een
afgedreven klok moet die meting verliezen, niet de veertien dagen. Afwijzingen
op berichtniveau worden naar `ingest_dead_letter` geschreven; die op
metingniveau komen terecht in `device_last.last_reject_reason`.

Twee situaties worden geteld maar nooit afgewezen: een interval met zowel afname
als injectie (doodgewoon op een bewolkte namiddag), en een onbekende sleutel
*binnen* een meting — §7 verbiedt die af te wijzen, en juist dat maakt het
toevoegen van een optioneel veld veilig.

## Inschrijving

Een toestel wordt aangemaakt door een gemeenschapsbeheerder, die een korte code
krijgt. Iemand aan de meter typt die code één keer in de connector:

```
POST /live-public/enroll     { "token": "…", "connector": { … } }
-> { "broker": {…}, "credentials": {…}, "topics": {…} }
```

Dit is het **enige niet-geauthenticeerde eindpunt** van het platform. Het token
is 128 bit CSPRNG-uitvoer in Crockford-base32, opgeslagen als SHA-256-hash en
eenmalig bruikbaar. Het alfabet vangt de verwarringen bij het voorlezen op — `I`
en `L` worden `1`, `O` wordt `0` — want het realistische geval is een code die
door de telefoon wordt gedicteerd in een captive portal, op een koude avond.

**Het wachtwoord wordt één keer getoond en is nooit terug te halen.** OptimCE
bewaart het niet; de broker houdt alleen een hash bij. Een verloren geheim
betekent opnieuw inschrijven — daarom is de unieke index over de meters van een
gemeenschap partieel: een ingetrokken toestel geeft zijn EAN weer vrij.

Een verlopen token, een reeds gebruikt token en een token dat nooit bestond
leveren een *identiek* antwoord op. Een onderscheidbaar antwoord zou een
aanvaller verklappen dat een gok juist was.

## Intrekking

Onmiddellijk en aan serverzijde: de brokerclient wordt uitgeschakeld, het
retained `status`-bericht gewist en de client verwijderd. Een ingetrokken
toestel wordt binnen milliseconden losgekoppeld en mag niet opnieuw verbinden.
Er is geen actie aan toestelzijde en geen melding — vanuit de connector gezien
houden zijn inloggegevens simpelweg op te werken.

Het wissen van de retained status is de stap die je makkelijk vergeet en duur
vergeet: een `status` die zijn toestel overleeft, wordt bij elke herverbinding
opnieuw aan de innameworker geleverd, zodat het offline-alarm bij elke
uitrol afgaat tot het team het niet meer leest.

## Eindpunten

| Methode | Pad | Doel |
|---|---|---|
| `GET` | `/live/version` | build- en schemaversie |
| `GET` | `/live/devices` | de toestellen van de gemeenschap |
| `POST` | `/live/devices` | er een aanmaken — valideert de EAN tegen het CRM |
| `POST` | `/live/devices/{id}/token` | een inschrijvingscode uitgeven (maakt een ongebruikte ongeldig) |
| `POST` | `/live/devices/{id}/revoke` | intrekken |
| `GET` | `/live/devices/{id}/diagnostics` | waarom een toestel zwijgt — toestand, `diag`, connector |
| `GET` | `/live/summary` | het huidige signaal van de gemeenschap. **LID-ondergrens** |
| `GET` | `/live/series` | een geaggregeerde reeks. **LID-ondergrens** |
| `GET` | `/live/settings` | zichtbaarheid en k. Geeft de standaardwaarden zonder te schrijven |
| `PUT` | `/live/settings` | ze vervangen |
| `GET` | `/live/forecast` | leeg **met een benoemde reden** zolang er geen methode is |
| `GET` | `/live/forecast/methods` | het register, momenteel leeg |
| `GET` | `/live/ops/health` | de vloot, en hoe oud de aggregaties zijn |
| `POST` | `/live-public/enroll` | **publiek.** Een code inruilen voor inloggegevens |

### Authenticatie

Alles onder `/live` wordt geauthenticeerd aan de gateway, die de JWT valideert
en `x-user-id`, `x-community-id` en `x-user-orgs` doorgeeft. De dienst
vertrouwt die headers en beperkt elke query tot de gemeenschap van de aanroeper;
de toegang is bovendien afhankelijk van een actief abonnement van de
gemeenschap. Zolang dat abonnement inactief is, negeert de worker ook de
telemetrie van de gemeenschap (statusberichten worden nog verwerkt) en slaat de
scheduler haar over zodra haar openstaande aggregaties klaar zijn; toestellen en
historiek blijven bewaard, zodat heractiveren geen nieuwe inschrijving vraagt.

`/live-public/enroll` is de uitzondering en wordt bereikt via een aparte
gateway-ingang zonder validator. De gateway kan de vertrouwensheaders niet voor
die ene route verwijderen, dus maakt de reverse proxy ze leeg voordat het
verzoek aankomt — een toestel heeft een inschrijvingstoken, nooit een sessie.

## Configuratie

| | |
|---|---|
| `CRM_DATABASE_URL` | het centrale CRM, alleen-lezen |
| `LOCAL_DATABASE_URL` | de eigen databank van deze dienst |
| `SUBSCRIPTION_CACHE_TTL_SECONDS` | hoe lang de worker en de scheduler hun kopie van de geabonneerde gemeenschappen vertrouwen (standaard 60, 1–3600) |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_TLS` | de broker die **deze dienst belt** |
| `MQTT_ADMIN_USERNAME` / `_PASSWORD` | de dynamic-security-stuurverbinding |
| `MQTT_INGEST_USERNAME` / `_PASSWORD` | de abonnee-identiteit van de worker |
| `BROKER_PUBLIC_HOST`, `_PORT`, `_TLS` | het adres dat **aan een toestel wordt verteld** |

De laatste twee regels zijn twee verschillende dingen en moeten dat blijven. Een
toestel schrijft `BROKER_PUBLIC_HOST` naar flash en vraagt er nooit meer naar;
ze samenvoegen tot één variabele zou in ontwikkeling perfect werken en daarna
een bezoek ter plaatse vergen bij elk ingeschreven toestel.

De innamedrempels (`INGEST_MAX_FUTURE_SECONDS`, `INGEST_MAX_AGE_DAYS`,
`INGEST_MAX_BATCH`, …) staan standaard op de waarden die het bevroren protocol
noemt. Er een wijzigen verandert wat conforme connectoren is toegezegd — wijzig
dus eerst het protocol.

## Uitvoeren

De broker is niet optioneel en niet gewoon: Mosquitto met de
**dynamic-security-plug-in**, want deze dienst maakt brokerclients aan en
verwijdert ze tijdens het draaien. Gebruik de ontwikkelstack, die de broker,
beide databanken en de gateway aan elkaar knoopt:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

De API staat dan op <http://localhost:8008>, met daarnaast een container
`live-data-worker` die MQTT verwerkt.

Om zonder hardware te werken is er een connectorsimulator. De poort van de
broker is alleen op de loopback van de host gepubliceerd, maar de simulator draait
het eenvoudigst binnen de stack:

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev run --rm --no-deps \
  live-data python scripts/simulate_device.py --enroll-token ABCD-… --profile pv-day
```

De profielen dekken een pv-dag, een store-and-forward-achterstand, één bericht
per afwijzingsreden, een te grote payload en klokafwijking.

## Tests

```bash
pytest                    # vereist een Docker-PostgreSQL op poort 5433
ruff check .
ruff format --check .
mypy .
```

`ruff check` en `ruff format --check` zijn aparte poorten: een regel van 110
tekens is een lintfout die de formatter al opgemaakt vindt, en verkeerd gekozen
aanhalingstekens zijn het omgekeerde.

**Geen enkele test benadert een broker, en dat is bewust.** GitHub Actions maakt
servicecontainers aan *voordat* de repository wordt uitgecheckt, dus een in de
repository opgenomen `mosquitto.conf` kan nooit hun bind-mountbron zijn — en
zonder dat bestand heeft de broker helemaal geen dynamische beveiliging. De
brokeradapter wordt hier daarom getest tegen een nagebootst transport, en de
helft die een echte broker nodig heeft, staat in de monorepo als
`scripts/verify-live-ingest.sh`: die toont aan dat een ingetrokken toestel echt
wordt losgekoppeld, dat een retained status echt wordt gewist, en dat een
opnieuw verzonden achterstand echt overschrijft.

## Schema

`scripts/sql/schema.sql` is ruwe DDL voor een NIEUWE databank;
`scripts/sql/migrations/` bevat de genummerde, enkel voorwaartse en herhaalbare
bestanden voor een levende. Er is geen Alembic en geen runner — migraties worden
met de hand of via `provision.sh` toegepast, in naamvolgorde. Dezelfde
instructies staan op beide plaatsen, en `tests/test_schema_migration_parity.py`
voorziet uit elk een databank en vergelijkt genormaliseerde catalogusmomentopnamen,
wat ze gelijk houdt. `shared/models/local_models.py` spiegelt het schema met de
hand: een wijziging aan het ene is een wijziging aan beide. `measurement` is per maand
bereikgepartitioneerd met een DEFAULT-partitie; een niet-lege standaardpartitie
betekent dat het partitiewerk is stilgevallen, en `/health/readiness` meldt dat.

## Bijdragen

Bijdragen zijn welkom! Lees de [bijdragerichtlijnen](../CONTRIBUTING.md) en onze
[gedragscode](../CODE_OF_CONDUCT.md) (in het Engels) voordat u een issue of pull
request opent.

## Beveiliging

Om een beveiligingslek te melden, volgt u het
[beveiligingsbeleid](../SECURITY.md) — open geen publieke issue.

## Licentie

Dit project valt onder de [Apache-licentie 2.0](../LICENSE).
