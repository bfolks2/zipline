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

    def get_time_order_held(self, current_time):
        return current_time - self.received_time


class Hospital(object):
    def __init__(self, name, coordinates):
        self.name = name
        self.x = coordinates[0]
        self.y = coordinates[1]

    def __str__(self):
        return u'{} @ ({}, {})'.format(self.name, self.x, self.y)

    def get_distance_to_origin(self):
        return self._distance_formula_km(x1=0, y1=0, x2=self.x, y2=self.y)

    def get_distance_to_other_coordinates(self, x_coord, y_coord):
        return self._distance_formula_km(x1=x_coord, y1=y_coord, x2=self.x, y2=self.y)

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
    def __init__(self, order_arr, start_time, distance):
        self.order_arr = order_arr
        self.start_time = start_time  # Assuming that flights start immediately after scheduled
        self.distance = distance  #kilometers

    def __str__(self):
        return u'{} Orders, Start Time: {}'.format(len(self.order_arr), self.start_time)

    def get_hospital_list_text(self):
        return ', '.join([order.hospital.name for order in self.order_arr])

    def get_projected_end_time(self):
        total_time = (self.distance * 1000) / ZipScheduler.MAX_SPEED
        return self.start_time + total_time


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
    MAX_SPEED = 30  # meters per second
    MAX_DELIVERIES = 3
    MAX_RANGE = 160  # kilometers
    MIN_RANGE = 160 * .85
    MAX_TIME_ORDER = 18000
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

    def get_available_zips(self, current_time):
        return (zip_obj for zip_obj in self.zip_database if zip_obj.is_available(current_time))

    def get_available_resupply_zips(self, current_time):
        return (zip_obj for zip_obj in self.resupply_zip_database if zip_obj.is_available(current_time))

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
            flight_zip = next(self.get_available_zips(current_time), None)
            if not flight_zip:
                return None

            # If possible/necessary, add resupply runs to the end of the Emergency
            if len(self.order_queue):
                order_arr, emergency_distance = self.append_to_emergency_order(order)
            else:
                emergency_distance = order.hospital.get_distance_to_origin() * 2
                order_arr = [order]

            flight_obj = Flight(order_arr=order_arr, start_time=current_time, distance=emergency_distance)
            flight_zip.set_flight(flight_obj)
            scheduled_flights.append(flight_obj)
            self.emergency_order_queue.pop(0)  # Remove the processed Order from the queue

        resupply_flight_zip = next(self.get_available_resupply_zips(current_time), None)
        while len(self.order_queue) and resupply_flight_zip:
            resupply_order_arr, resupply_distance = self.compile_resupply_order(current_time)
            if resupply_order_arr:
                flight_obj = Flight(order_arr=resupply_order_arr, start_time=current_time, distance=resupply_distance)
                resupply_flight_zip.set_flight(flight_obj)
                scheduled_flights.append(flight_obj)

                self.order_queue = [order for order in self.order_queue if order not in resupply_order_arr]
                resupply_flight_zip = next(self.get_available_resupply_zips(current_time), None)
            else:
                break

        return [flight_obj.get_hospital_list_text() for flight_obj in scheduled_flights]

    def compile_resupply_order(self, current_time):
        distance = self.MAX_RANGE + 1  # Set the starting point over the max range
        resupply_order_arr = []
        if len(self.order_queue) > self.MAX_DELIVERIES:
            resupply_order_queue = self.sort_queue()
        else:
            resupply_order_queue = self.order_queue

        while distance > self.MAX_RANGE and resupply_order_queue:
            perms = list(permutations(resupply_order_queue))
            for perm in perms:
                perm_distance = 0
                last_x = 0
                last_y = 0
                for i, order in enumerate(perm):
                    perm_distance += order.hospital.get_distance_to_other_coordinates(last_x, last_y)
                    if i == len(perm) - 1:
                        # Return to origin
                        perm_distance += order.hospital.get_distance_to_origin()
                        break
                    last_x = order.hospital.x
                    last_y = order.hospital.y
                if perm_distance < distance:
                    distance = perm_distance
                    resupply_order_arr = list(perm)
            if distance > self.MAX_RANGE:
                resupply_order_queue.pop()  # Remove the most recent Resupply and try again

        # Only send orders of 1 Item if they above the MIN_RANGE or past the MAX_TIME_ORDER
        if len(resupply_order_arr) == 1:
            lone_order = resupply_order_arr[0]
            if distance < self.MIN_RANGE and self.MAX_TIME_ORDER > lone_order.get_time_order_held(current_time):
                return None, distance

        return resupply_order_arr, distance

    def sort_queue(self):
        """
        Sort by priority and distance.  Start with the oldest order, then find its 2 closest matches
        :return:  order Array
        """
        starting_order = self.order_queue[0]
        starting_hospital = starting_order.hospital

        extra_order_arr = []
        for i, extra_order in enumerate(self.order_queue[1:]):
            extra_order_arr.append((extra_order.hospital.get_distance_to_other_coordinates(starting_hospital.x,
                                                                                           starting_hospital.y), i + 1))

        extra_order_arr.sort(key=lambda x: x[0])
        return [starting_order, self.order_queue[extra_order_arr[0][1]], self.order_queue[extra_order_arr[1][1]]]

    def append_to_emergency_order(self, emergency_order):
        distance = self.MAX_RANGE + 1
        order_index = None

        for i, resupply_order in enumerate(self.order_queue):
            i_distance = emergency_order.hospital.get_distance_to_origin()
            i_distance += emergency_order.hospital.get_distance_to_other_coordinates(resupply_order.hospital.x,
                                                                                     resupply_order.hospital.y)
            i_distance += resupply_order.hospital.get_distance_to_origin()
            if i_distance < distance:
                distance = i_distance
                order_index = i

        if distance <= self.MAX_RANGE and order_index is not None:
            return [emergency_order, self.order_queue.pop(order_index)], distance
        else:
            return [emergency_order], emergency_order.hospital.get_distance_to_origin() * 2

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
