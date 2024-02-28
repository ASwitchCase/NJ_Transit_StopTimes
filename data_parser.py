import sqlite3
import uuid


def get_stop_times(file):
    stops_list = []

    for line in file:
        line_data = line.split(",")
        stops_list.append({
            "trip_id":line_data[0],
            "arrival":line_data[1],
            "departure_time":line_data[2],
            "stop_id":line_data[3],
            "stop_sequence":line_data[4],
            "pickup_type":line_data[5],
            "drop_off_type":line_data[6],
            "shape_dist_traveled":line_data[7]
        })
    stops_list.pop(0)
    return stops_list

def get_stops(file):
    stops_list = []

    for line in file:
        line_data = line.split(",")
        stops_list.append({
            "stop_id":line_data[0],
            "stop_code":line_data[1],
            "stop_name":line_data[2],
            "stop_lat":line_data[4],
            "stop_lon":line_data[5],
            "zone_id":line_data[6],
        })
    stops_list.pop(0)
    return stops_list

def create_database():
    db = sqlite3.connect('NJTDB.sqlite3')
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS STOP_TIMES")
    cursor.execute("DROP TABLE IF EXISTS STOPS")
    CREATE_TABLE_STOP_TIMES = """CREATE TABLE STOP_TIMES (
            stop_time_id VARCHAR(255) NOT NULL,
            trip_id VARCHAR(255) NOT NULL,
            arrival VARCHAR(255) NOT NULL,
            departure_time VARCHAR(255) NOT NULL,
            stop_id VARCHAR(255) NOT NULL,
            stop_sequence VARCHAR(255) NOT NULL,
            pickup_type VARCHAR(255) NOT NULL,
            drop_off_type VARCHAR(255) NOT NULL,
            shape_dist_traveled VARCHAR(255) NOT NULL,
            PRIMARY KEY (stop_time_id),
            FOREIGN KEY (stop_id) REFERENCES STOPS(stop_id)
    );"""

    CREATE_TABLE_STOPS = """CREATE TABLE STOPS (
            stop_id VARCHAR(255) NOT NULL,
            stop_code VARCHAR(255) NOT NULL,
            stop_name VARCHAR(255) NOT NULL,
            stop_lat VARCHAR(255) NOT NULL,
            stop_lon VARCHAR(255) NOT NULL,
            zone_id VARCHAR(255) NOT NULL,
            PRIMARY KEY (stop_id)
    );"""

    cursor.execute(CREATE_TABLE_STOP_TIMES)
    cursor.execute(CREATE_TABLE_STOPS)
    cursor.close()

def populate_database():
    stops_times = get_stop_times(open("data/stop_times.txt","r"))
    stops = get_stops(open("data/stops.txt","r"))
    db = sqlite3.connect('NJTDB.sqlite3')
    cursor = db.cursor()
    
    for stop_time in stops_times:
        q = f'''
                       INSERT INTO STOP_TIMES VALUES (
                       '{str(uuid.uuid4())}',
                       '{stop_time['trip_id']}',
                       '{stop_time['arrival']}', 
                       '{stop_time['departure_time']}',
                       '{stop_time['stop_id']}', 
                       '{stop_time['stop_sequence']}', 
                       '{stop_time['pickup_type']}', 
                       '{stop_time['drop_off_type']}',
                       '{stop_time['shape_dist_traveled']}'
                       );
            '''
        cursor.execute(q) 

    for stop in stops:
        q = f'''
                       INSERT INTO STOPS VALUES (
                       '{stop['stop_id']}',
                       '{stop['stop_code']}',
                       "{stop['stop_name'][1:-1]}",  
                       '{stop['stop_lat']}',
                       '{stop['stop_lon']}', 
                       '{stop['zone_id']}' 
                       );
            '''
        cursor.execute(q) 
    db.commit()
    db.close()

create_database()
populate_database()