from datetime import datetime
import csv
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Order constants
ORDER_PRIORITIES = ('Resupply', 'Emergency')
PROCESSED = 1
IN_TRANSIT = 2
COMPLETE = 3
ORDER_STATUS = {PROCESSED: 'Processed', IN_TRANSIT: 'In Transit', COMPLETE: 'Complete'}

# Zip constants
MAX_SPEED = 30  # meters per section
MAX_DELIVERIES = 3
MAX_RANGE = 160  # kilometers
NUMBER_OF_ZIPS = 10
ZIP_AVAILABLE = 1
ZIP_INFLIGHT = 2

# Data items
hospital_list = []
all_order_list = []
order_queue = []
zip_dict = dict.fromkeys(range(1, (NUMBER_OF_ZIPS + 1)), ZIP_AVAILABLE)


class Order(object):
    def __init__(self, received_time, hospital_name, priority='Resupply'):
        if priority not in ORDER_PRIORITIES:
            raise TypeError("Invalid Order Priority chosen")
        self.received_time = received_time
        self.hospital = hospital_query('name', hospital_name)
        self.priority = priority
        self.status = PROCESSED

    def __str__(self):
        return u'{} Order for {}, {}'.format(self.priority, self.hospital.name, ORDER_STATUS.get(self.status))


class Hospital(object):
    def __init__(self, name, coordinates):
        self.name = name
        self.coordinates = coordinates  # X,Y in meters

    def __str__(self):
        return u'{} @ {}'.format(self.name, self.coordinates)


# class Zip(object):
#     max_speed = 30  # meters per section
#     max_deliveries = 3
#     max_range = 160  # kilometers
#
#     def __init__(self, id):
#         self.id = id
#         self.in_flight = False
#
#     def __str__(self):
#         return u'Zip {}, In Flight - {}'.format(self.id, self.in_flight)
#
#     def get_flight_status(self):
#         return self.in_flight
#
#     def set_flight_status(self, status):
#         self.in_flight = status


class Flight(object):
    def __init__(self, zip_id, order_arr):
        self.zip_id = zip_id
        self.order_arr = order_arr

        zip_dict[self.zip_id] = ZIP_INFLIGHT
        self.start_time = datetime.now()

    def __str__(self):
        return u'Zip #{}, {} Orders, Start Time: {}'.format(self.zip_id, len(self.order_arr), self.start_time)

    def end_flight(self):
        zip_dict[self.zip_id] = ZIP_AVAILABLE


def hospital_query(param, value):
    """
    Simplified Database query to find instances of a Hospital based on some param/value
    :return: First matching Hospital instance
    """
    for hospital in hospital_list:
        if getattr(hospital, param) == value:
            return hospital
    return None


# RUN CODE
file_path = os.path.join(PROJECT_ROOT, 'zipline/csv_data/hospitals.csv')
with open(file_path, 'r') as csvfile:
    csv_spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_spamreader:
        row_hospital = Hospital(name=row[0], coordinates=(int(row[1]), int(row[2])))
        hospital_list.append(row_hospital)

file_path = os.path.join(PROJECT_ROOT, 'zipline/csv_data/orders.csv')
with open(file_path, 'r') as csvfile:
    csv_spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_spamreader:
        row_order = Order(received_time=row[0], hospital_name=row[1].strip(), priority=row[2].strip())
        all_order_list.append(row_order)

print('Done')
