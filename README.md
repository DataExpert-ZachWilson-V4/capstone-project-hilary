# Passenger Forecasts

Airports are gateways to cities and are a major source of the regional economy. Efficiently transporting people though the airport requires a passenger forecast in order to most closely match the number of Transportation Security Association (TSA) personnel, the number and frequency of airport shuttle systems, and personnel for terminal services. Unfortunately, detailed passenger statistics are not available to airports when the day-ahead flight schedules are generated. Companies, like OAG, generate passenger forecasts from flight schedules using publically available data to estimate the number of passengers an airport will see during the hours of the day, enabling the airport to plan its operations efficiently.

The process of forecasting passengers involves combining several data sources. First, The Bureau of Transportation Statistics (BTS) publishes flight data that lags by a few months. For the purpose of this project, I will stream these values and treat them as a real-time source. This data can be merged with the Federal Aviation Administration (FAA) Releasable Aircraft Database to estimate the number of seats on each flight. Since all flights are not 100% full, the BTS T100 dataset provides monthly load factors for each carrier and route. This provides the best estimate of the number of passengers that utilize the airport during any given time period when linked with detailed flight data. Finally, some passengers only use the airport for transferring from one plane to another, but others will need the landside services that the airport provides. Combining BTS T100 and another data source, BTS DB1B, provides quarterly estimates of the passengers visiting the city.

Data Sources:

- Bureau of Transportation Statistics (BTS)
- Federal Aviation Administration (FAA)

Datasets used in project:

1. Reporting Carrier On-Time Performance (BTS)

- Report scheduled & actual departure/arrival times for non-stop domestic flights by month and year
- Includes canceled and diverted flights, causes of delay and cancelation, air time
- Includes US certified air carriers that account for at least half of one percent of domestic scheduled passenger revenues

2. T-100 Data (BTS)

- Monthly reporting (3 month lag for domestic, 6 month lag for international)
- On board passengers & cargo tons reported

3. Origin and Destination Data (BTS)

- Records passengers' point of origin, intermediate stops, each flight segment carrier and fare paid from origin to destination
- 10% sample (All passenger tickets with code ending in 0 are recorded) & raw data is multiplied by 10 to represent market
- Reported quarterly

4. Releasable Aircraft Database (FAA)

- Txt file containing detailed registered commerical aircraft information such as number of seats

5. Simulated data based on reporting carrier on-time performance data set

- Mock changes to arrivals and departures to facilitate real-time analysis opportunities airports have to understand passenger flow
