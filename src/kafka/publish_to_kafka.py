from confluent_kafka import Producer
import json
import os
import pandas as pd
import random
from time import sleep

from kafka_helpers import read_config


def produce(topic, config, size_limit, run_interval_min):
    # Simulate fake data flowing in from source
    fake_dt = (pd.Timestamp.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
    df = pd.read_parquet(
        os.environ["KAFKA_DATA_PATH"], filters=[("FlightDate", "=", fake_dt)]
    )
    if size_limit:
        n_records = min(size_limit, len(df))
        df = df.sample(n_records)
    else:
        n_records = len(df)
    max_delay = run_interval_min * 60 / n_records
    producer = Producer(config)
    while len(df) > 0:
        index = df.sample().index.item()
        record = df.loc[index]
        flight_date = record["FlightDate"]
        reporting_airline = record["Reporting_Airline"]
        flight_num = record["Flight_Number_Reporting_Airline"]
        key = f"{flight_date} {reporting_airline} {flight_num}"
        record_dict = record.to_dict()
        for k in [
            "Flight_Number_Reporting_Airline",
            "OriginAirportID",
            "OriginCityMarketID",
            "OriginWac",
            "DestAirportID",
            "DestCityMarketID",
            "DestWac",
            "CRSDepTime",
            "DepTime",
            "DepDelay",
            "TaxiOut",
            "WheelsOff",
            "WheelsOn",
            "TaxiIn",
            "CRSArrTime",
            "ArrTime",
            "ArrDelay",
            "Cancelled",
            "Diverted",
            "CRSElapsedTime",
            "ActualElapsedTime",
            "AirTime",
            "Flights",
            "Distance",
            "CarrierDelay",
            "WeatherDelay",
            "NASDelay",
            "SecurityDelay",
            "LateAircraftDelay",
            "TotalAddGTime",
            "LongestAddGTime",
            "DivAirportLandings",
            "DivReachedDest",
            "DivActualElapsedTime",
            "DivArrDelay",
            "DivDistance",
        ]:
            if pd.notnull(record_dict[k]):
                record_dict[k] = int(record_dict[k])
        value = json.dumps(record_dict).encode("utf-8")
        producer.produce(
            topic,
            key=key,
            value=value,
        )
        df = df.query("index != @index")
        sec_delay = random.uniform(0, max_delay)
        print(
            f"Produced record with key: {key}; {sec_delay/60:.4f}min delay before sending next fake record"
        )
        sleep(sec_delay)
    producer.flush()


def main(topic, size_limit=5, run_interval_min=20):
    config = read_config()
    produce(topic, config, size_limit=size_limit, run_interval_min=run_interval_min)


if __name__ == "__main__":
    main(topic=os.environ["KAFKA_FLIGHTS_TOPIC"], size_limit=None, run_interval_min=10)
