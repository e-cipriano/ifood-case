create table tripdata.yellow_tripdata (

    dolocation_id           string ,
    ratecode_id             string ,
    fare_amount             string ,
    tpep_dropoff_datetime   string ,
    congestion_surcharge    string ,
    vendor_id               string , 
    passenger_count         string ,
    tolls_amount            string , 
    airport_fee             string ,
    improvemwent_surcharge  string ,
    trip_distance           string , 
    store_and_fwd_flag      string ,
    payment_type            string ,
    total_amount            string ,
    extra                   string ,
    tip_amount              string ,
    mta_tax                 string ,
    tpep_pickup_datetime    string 

)
cluster by vendor_id, date (tpep_pickup_datetime)