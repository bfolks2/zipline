from datetime import datetime
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
        self.coordinates = coordinates  # X,Y in meters

    def __str__(self):
        return u'{} @ {}'.format(self.name, self.coordinates)


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
    def __init__(self, zip_id, order_arr, start_time):
        self.zip_id = zip_id
        self.order_arr = order_arr
        self.start_time = start_time  # Assuming that flights start immediately after scheduled

    def __str__(self):
        return u'Zip #{}, {} Orders, Start Time: {}'.format(self.zip_id, len(self.order_arr), self.start_time)

    def get_flight_length(self):
        pass

    def get_projected_end_time(self):
        pass


class ZipScheduler(object):
    # Zip constants
    MAX_SPEED = 30  # meters per section
    MAX_DELIVERIES = 3
    MAX_RANGE = 160  # kilometers
    NUMBER_OF_ZIPS = 10

    # Live queues
    order_queue = []
    scheduled_flights = []
    zip_dict = dict.fromkeys(range(1, (NUMBER_OF_ZIPS + 1)), None)

    def queue_order(self, received_time, hospital, priority):
        order_obj = Order(received_time, hospital, priority)

        # Only append to the order_queue if we successfully matched a Hospital
        if order_obj.hospital:
            self.order_queue.append(order_obj)

    def schedule_next_flight(self, current_time):
        if not len(self.order_queue):
            return None

        flight_obj = Flight(zip_id=1, order_arr=self.order_queue, start_time=current_time)
        self.scheduled_flights.append(flight_obj)
        self.order_queue = []


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
start_time = int(all_orders_list[0].get('received_time'))
end_time = int(all_orders_list[len(all_orders_list) - 1].get('received_time'))
call_times = range(start_time, end_time + 60, 60)

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
    zip_scheduler.schedule_next_flight(current_time=call_time)

print('Done')
