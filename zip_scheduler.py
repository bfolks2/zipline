from itertools import permutations
from math import sqrt
import csv
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# Data arrays
hospital_database = []
all_orders_list = []


class Order(object):
    order_priorities = ('Resupply', 'Emergency')

    def __init__(self, received_time, hospital, priority='Resupply'):
        if priority not in self.order_priorities:
            raise TypeError("Invalid Order Priority chosen")
        self.received_time = received_time
        self.hospital = hospital_query('name', hospital)
        self.priority = priority

    def __str__(self):
        return u'{} Order for {}, {}'.format(self.priority, self.hospital.name, self.received_time)


class Hospital(object):
    def __init__(self, name, coordinates):
        self.name = name
        self.x = coordinates[0]
        self.y = coordinates[1]

    def __str__(self):
        return u'{} @ ({}, {})'.format(self.name, self.x, self.y)

    def get_distance_to_origin(self):
        return self._distance_formula_km(x1=0, y1=0, x2=self.x, y2=self.y)

    def get_distance_to_other_hospital(self, other_hospital):
        return self._distance_formula_km(x1=other_hospital.x, y1=other_hospital.y, x2=self.x, y2=self.y)

    @staticmethod
    def _distance_formula_km(x1, y1, x2, y2):
        return sqrt(((x2 - x1) ** 2) + ((y2 - y1) ** 2)) / 1000


def hospital_query(param, value):
    """
    Simplified Database query to find instances of a Hospital based on some param/value
    :return: First matching Hospital instance
    """
    for hospital in hospital_database:
        if getattr(hospital, param) == value:
            return hospital
    return None


class Flight(object):
    def __init__(self, order_arr, start_time):
        self.order_arr = order_arr
        self.start_time = start_time  # Assuming that flights start immediately after scheduled

    def __str__(self):
        return u'{} Orders, Start Time: {}'.format(len(self.order_arr), self.start_time)

    def get_projected_end_time(self):
        return 0


class Zip(object):
    def __init__(self, key):
        self.id = key
        self.flight = None

    def __str__(self):
        return u'Zip #{}'.format(self.id)

    def set_flight(self, flight):
        self.flight = flight

    def is_available(self, current_time):
        if not self.flight:
            return True

        available_time = self.flight.get_projected_end_time()
        return True if current_time >= available_time else False


class ZipScheduler(object):
    # Zip constants
    MAX_SPEED = 30  # meters per section
    MAX_DELIVERIES = 3
    MAX_RANGE = 160  # kilometers
    NUMBER_OF_ZIPS = 10  # Can be used for either Resupply or Emergency
    EMERGENCY_ONLY_ZIPS = 2  # Always keep 2 Zips available for Emergency purposes only

    # Live queues
    order_queue = []
    emergency_order_queue = []

    def __init__(self):
        self.zip_database = []
        for i in range(0, self.NUMBER_OF_ZIPS):
            self.zip_database.append(Zip(key=i+1))
        self.resupply_zip_database = self.zip_database[:-self.EMERGENCY_ONLY_ZIPS]

    def queue_order(self, received_time, hospital, priority):
        order_obj = Order(received_time, hospital, priority)

        # Only append to the order_queue if we successfully matched a Hospital
        if order_obj.hospital:
            if order_obj.priority == 'Emergency':
                self.emergency_order_queue.append(order_obj)
            else:
                self.order_queue.append(order_obj)

    def schedule_next_flight(self, current_time):
        scheduled_flights = []

        # If not orders exists, return None
        if not len(self.order_queue) and not len(self.emergency_order_queue):
            return None

        for order in self.emergency_order_queue:
            # Assign an available Zip for each Emergency order, if possible
            flight_zip = next((zip for zip in self.zip_database if zip.is_available(current_time)), None)
            if not flight_zip:
                return None

            flight_obj = Flight(order_arr=[order], start_time=current_time)
            flight_zip.set_flight(flight_obj)
            scheduled_flights.append(flight_obj)
            self.emergency_order_queue.pop(0)  # Remove the processed Order from the queue

        # If only Emergency flights were ordered, return an array of them
        if not len(self.order_queue):
            return [flight_obj.order_arr[0].hospital.name for flight_obj in scheduled_flights]

        perms = list(permutations(self.order_queue))
        for perm in perms:
            pass


# *******************************************************************************************
# **************************************** TEST CODE ****************************************
# *******************************************************************************************

# First populate a Hospital array from the CSV data to mock out a "Hospital Database"
# The order_array is left intentionally in primitive format, to more closely mimic real-time
file_path = os.path.join(PROJECT_ROOT, 'zipline/csv_data/hospitals.csv')
with open(file_path, 'r') as csvfile:
    csv_spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_spamreader:
        row_hospital = Hospital(name=row[0], coordinates=(int(row[1]), int(row[2])))
        hospital_database.append(row_hospital)

# Because this test is not in real-time, we will populate an Order information array using the CSV Data
# The order_array is left intentionally in primitive format, to more closely mimic real-time
file_path = os.path.join(PROJECT_ROOT, 'zipline/csv_data/orders.csv')
with open(file_path, 'r') as csvfile:
    csv_spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in csv_spamreader:
        row_order_dict = {'received_time': int(row[0]), 'hospital': row[1].strip(), 'priority': row[2].strip()}
        all_orders_list.append(row_order_dict)

# Starting with the first received_time, scan through all remaining values in the all_orders_list every 60 seconds
# Queue any orders with a received time less than the current time
orders_start_time = int(all_orders_list[0].get('received_time'))
orders_end_time = int(all_orders_list[len(all_orders_list) - 1].get('received_time'))
call_times = range(orders_start_time, orders_end_time + 60, 60)

zip_scheduler = ZipScheduler()
for call_time in call_times:
    # Queue any orders that were received in the last 60 seconds
    while True:
        # If the all_orders_list is empty, break the for-loop
        if not len(all_orders_list):
            break

        order_dict = all_orders_list[0]
        if order_dict.get('received_time') <= call_time:
            zip_scheduler.queue_order(received_time=order_dict.get('received_time'),
                                      hospital=order_dict.get('hospital'),
                                      priority=order_dict.get('priority'))
            all_orders_list.pop(0)
        else:
            break

    # Call logic to schedule a flight, if necessary
    flight_hospital_list = zip_scheduler.schedule_next_flight(current_time=call_time)

print('Done')
