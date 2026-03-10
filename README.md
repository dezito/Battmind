
## :warning: <mark>**Dette projekt er i alpha stadie**</mark> :warning:

# BattMind🤵‍♂️🔋⚡ til Home Assistant (PyScript)

### Beskrivelse:
Styring af husbatteri

### :warning: <mark>**Home Assistant Standard database, SQLite understøttes ikke**</mark> :warning:
  - Installere f.eks. MariaDB og evt. InfluxDB
    - [Konvertere til MariaDB](https://theprivatesmarthome.com/how-to/use-mariadb-instead-of-sqlite-db-in-home-assistant/)
    - [Installere InfluxDB](https://pimylifeup.com/home-assistant-influxdb/)
    - [Optimimere Home Assistant Database](https://smarthomescene.com/guides/optimize-your-home-assistant-database/)
  - pga. fejl i homeassistant.components.recorder.history bibliotek
### Påkrævet integrationer
- [HACS](https://github.com/hacs/integration)
- [PyScript](https://github.com/custom-components/pyscript)
  - Allow All Imports - Aktiveret
  - Access hass as a global variable - Aktiveret
- [Energi Data Service](https://github.com/MTrab/energidataservice)
  - [Carnot](https://www.carnot.dk/) - Aktiveret (AI elpriser prognose)
- [Sun](https://www.home-assistant.io/integrations/sun/)
- Vejr prognose integration
  - [AccuWeather](https://www.home-assistant.io/integrations/accuweather/)
  - [Meteorologisk institutt](https://www.home-assistant.io/integrations/met/)
  - [OpenWeather](https://www.home-assistant.io/integrations/openweathermap/)
  - Andre med temperatur, skydække og som understøtter weather.get_forecasts service kald

### Installation:
1. Kopiere koden her under og sæt ind i Terminal eller SSH, Battmind installeres og nødvendig ændringer i configuration.yaml tilføjes automatisk
```shell
curl -s https://raw.githubusercontent.com/dezito/Battmind/refs/heads/main/scripts/update_battmind.sh | bash
```

2. Ved første start vil den lave en yaml konfig fil (battmind_config.yaml) i roden af Home Assistant mappen
3. Redigere denne efter dit behov
4. Genstart Home Assistant
    - Ved anden start vil den lave en yaml fil i packages mappen (packages\battmind.yaml) med alle entities scriptet bruger
      - Dette variere afhængig af om der er integrationer til solceller, husbatteri osv. der bliver registreret i konfig filen
      - Alle entitier navne starter med battmind_ der laves
5. Genstart Home Assistant