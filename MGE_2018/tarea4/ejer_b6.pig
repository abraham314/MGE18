flights = LOAD 's3://tarea4/bases/flights.csv' using PigStorage(',') as (year:float, month:int, day:int, day_of_week:int, airline:chararray, flight_number:float, tail_numer:chararray, origin_airport:chararray, destination_airport:chararray, scheduled_departure:chararray, departure_time: chararray, departure_delay:int, taxi_out:int, wheels_off:chararray, scheduled_time:float, elapsed_time:float, air_time:float, distance:float, wheels_on:chararray, taxi_in:int, scheduled_arrival:chararray, arrival_time:chararray, arrival_delay:int, diverted:int, cancelled:int, cancelation_reason:chararray, air_system_delay:chararray, security_delay:chararray, airline_delay:chararray, late_aircraft_delay:chararray, weather_delay:chararray);

airports = LOAD 's3://tarea4/bases/airports.csv' using PigStorage(',') as (iata_code:chararray, airport:chararray, city:chararray, state:chararray, country:chararray, latitud:float, longitude:float);

grp_vuelos = group flights by flight_number; 


n_dest = foreach grp_vuelos { 
    unique_dest = DISTINCT flights.destination_airport;
    generate group, COUNT(unique_dest) as dest_cnt;
};

ranking =  order n_dest by dest_cnt DESC; 
top1 = limit ranking 1; 
join_top1 = JOIN flights by flight_number,top1 by group; 
aerotop = foreach join_top1 generate flight_number,destination_airport; 
aero_dist = DISTINCT aerotop; 
aero_desf = JOIN aero_dist by destination_airport, airports by iata_code;

STORE aero_desf INTO 's3://tarea4/outputs/ejer_b6' USING PigStorage(',');
