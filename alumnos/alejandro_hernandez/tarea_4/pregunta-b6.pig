flights = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);
airports = LOAD 's3://aws-alex-03032018-metodos-gran-escala/datos/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);

unicos_aero =
    FOREACH (GROUP flights BY flight_number) {
        dest=flights.destination_airport;
        dist_dest=distinct dest;
        GENERATE group as flight_number, COUNT(dist_dest) as n;
    };

ord_unicos_aero = ORDER unicos_aero by n DESC;
max_aero= limit ord_unicos_aero 1;

destinos = FOREACH flights generate flight_number,destination_airport;
destinos_dist = distinct destinos;
destinos_dist_aero = JOIN destinos_dist by destination_airport, airports by iata_code;

tabla_dest = JOIN max_aero by flight_number LEFT OUTER, destinos_dist_aero by flight_number;
STORE tabla_dest INTO 's3://aws-alex-03032018-metodos-gran-escala/output/pregunta_b6' USING PigStorage(';');
